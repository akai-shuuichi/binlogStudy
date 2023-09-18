package org.example;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.example.deserializer.ByteArrayEventData;
import org.example.deserializer.Event;
import org.example.deserializer.EventData;
import org.example.deserializer.EventDataDeserializer;
import org.example.deserializer.EventHeader;
import org.example.deserializer.EventHeaderV4;
import org.example.deserializer.EventHeaderV4Deserializer;
import org.example.deserializer.EventType;
import org.example.deserializer.QueryEventData;
import org.example.deserializer.QueryEventDataDeserializer;
import org.example.io.ByteArrayInputStream;
import org.example.protocol.ErrorPacket;
import org.example.protocol.GreetingPacket;
import org.example.protocol.Packet;
import org.example.protocol.PacketChannel;
import org.example.protocol.ResultSetRowPacket;
import org.example.protocol.command.AuthenticateCommand;
import org.example.protocol.command.Command;
import org.example.protocol.command.DumpBinaryLogCommand;
import org.example.protocol.command.QueryCommand;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.rmi.ServerException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static String binlogFilename;

    public static Long binlogPosition;

    public static ChecksumType checksumType;
//fetchBinlogFilenameAndPosition
    public static void main(String[] args) throws Exception {
        PacketChannel packetChannel = openConnect();

        GreetingPacket greetingPacket = receiveGreeting(packetChannel);

        System.out.println(JSONUtil.toJsonStr(greetingPacket));
        login(packetChannel, greetingPacket);
        long connectionId = greetingPacket.getThreadId();

        fetchBinlogFilenameAndPosition(packetChannel);



//        System.out.println(binlogFilename+" "+binlogPosition);
//
         checksumType = fetchBinlogChecksum(packetChannel);
        confirmSupportOfChecksum(packetChannel, checksumType);
        requestBinaryLogStream(packetChannel);
//
        readMessage(packetChannel);
    }


    private static PacketChannel openConnect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("10.224.128.20", 3306), 10000);
        PacketChannel packetChannel = new PacketChannel(socket);
        return packetChannel;
    }

    private static GreetingPacket receiveGreeting(final PacketChannel channel) throws IOException {
        byte[] initialHandshakePacket = channel.read();
        if (initialHandshakePacket[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(initialHandshakePacket, 1, initialHandshakePacket.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new RuntimeException(JSONUtil.toJsonStr(errorPacket));
        }
        return new GreetingPacket(initialHandshakePacket);
    }

    private static void login(PacketChannel channel, GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();
        int packetNumber = 1;
        AuthenticateCommand authenticateCommand = new AuthenticateCommand(null, "root", "123gzx", greetingPacket.getScramble());
        authenticateCommand.setCollation(collation);
        channel.write(authenticateCommand, packetNumber);
        byte[] authenticationResult = channel.read();
        if (authenticationResult[0] != (byte) 0x00 /* ok */) {
            throw new RuntimeException("error");
        }
    }

    private static ResultSetRowPacket[] readResultSet(final PacketChannel channel) throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList<>();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new RuntimeException(JSONUtil.toJsonStr(errorPacket));
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; ) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[0]);
    }

    private static void fetchBinlogFilenameAndPosition(PacketChannel channel) throws IOException {
        channel.write(new QueryCommand("show master status"));
        ResultSetRowPacket[] resultSet = readResultSet(channel);

        if (resultSet.length == 0) {
            throw new IOException("Failed to determine binlog filename/position");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        binlogFilename = resultSetRow.getValue(0);
        binlogPosition = Long.parseLong(resultSetRow.getValue(1));
    }

    private static ChecksumType fetchBinlogChecksum(final PacketChannel channel) throws IOException {
        channel.write(new QueryCommand("show global variables like 'binlog_checksum'"));
        ResultSetRowPacket[] resultSet = readResultSet(channel);
        if (resultSet.length == 0) {
            return ChecksumType.NONE;
        }
        return ChecksumType.valueOf(resultSet[0].getValue(1).toUpperCase());
    }

    private static void confirmSupportOfChecksum(final PacketChannel channel, ChecksumType checksumType) throws IOException {
        channel.write(new QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"));
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new RuntimeException(JSONUtil.toJsonStr(errorPacket));
        }
//        eventDeserializer.setChecksumType(checksumType);
    }

    private static void requestBinaryLogStream(final PacketChannel channel) throws IOException {
        long serverId = 65531; // http://bugs.mysql.com/bug.php?id=71178
        Command dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, 4);
        channel.write(dumpBinaryLogCommand);
    }

    public static void readMessage(PacketChannel channel) throws IOException {
        ByteArrayInputStream inputStream = channel.getInputStream();
        try {
            while (inputStream.peek() != -1) {
                int packetLength = inputStream.readInteger(3);
                //noinspection ResultOfMethodCallIgnored
                inputStream.skip(1); // 1 byte for sequence
                int marker = inputStream.read();
                if (marker == 0xFF) {
                    ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
                    throw new RuntimeException(JSONUtil.toJsonStr(errorPacket));
                }

                Event event = null;
                try {
                    event = nextEvent(packetLength == 16777215 ? new ByteArrayInputStream(readPacketSplitInChunks(inputStream, packetLength - 1)) : inputStream);
                    if (event == null) {
                        throw new EOFException();
                    }
                } catch (Exception e) {

                }
            }
        } catch (Exception e) {
        }
    }

    private static byte[] readPacketSplitInChunks(ByteArrayInputStream inputStream, int packetLength) throws IOException {
        byte[] result = inputStream.read(packetLength);
        int chunkLength;
        do {
            chunkLength = inputStream.readInteger(3);
            //noinspection ResultOfMethodCallIgnored
            inputStream.skip(1); // 1 byte for sequence
            result = Arrays.copyOf(result, result.length + chunkLength);
            inputStream.fill(result, result.length - chunkLength, chunkLength);
        } while (chunkLength == Packet.MAX_LENGTH);
        return result;
    }

    private static Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
        if (inputStream.peek() == -1) {
            return null;
        }

        EventHeaderV4Deserializer eventHeaderV4Deserializer = new EventHeaderV4Deserializer();
        EventHeader eventHeader = eventHeaderV4Deserializer.deserialize(inputStream);
        System.out.println("事件"+eventHeader.getEventType());
        if(EventType.QUERY.equals(eventHeader.getEventType())) {
            int eventBodyLength = (int) eventHeader.getDataLength() - checksumType.getLength();
            inputStream.enterBlock(eventBodyLength);


            QueryEventDataDeserializer queryEventDataDeserializer = new QueryEventDataDeserializer();
            QueryEventData deserialize = queryEventDataDeserializer.deserialize(inputStream);


            System.out.println(JSONUtil.toJsonStr(deserialize));

            inputStream.skipToTheEndOfTheBlock();
            inputStream.skip(checksumType.getLength());

            return new Event(eventHeader, deserialize);
        }
        byte[] read = inputStream.read((int) eventHeader.getDataLength());
        return new Event(eventHeader, null);
    }

}