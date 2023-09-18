/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.protocol;


import cn.hutool.json.JSONUtil;
import org.example.io.BufferedSocketInputStream;
import org.example.io.ByteArrayInputStream;
import org.example.io.ByteArrayOutputStream;
import org.example.protocol.command.AuthenticateCommand;
import org.example.protocol.command.Command;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.Channel;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class PacketChannel implements Channel {

    private Socket socket;
    private ByteArrayInputStream inputStream;
    private ByteArrayOutputStream outputStream;

    public PacketChannel(String hostname, int port) throws IOException {
        this(new Socket(hostname, port));
    }

    public PacketChannel(Socket socket) throws IOException {
        this.socket = socket;
        this.inputStream = new ByteArrayInputStream(new BufferedSocketInputStream(socket.getInputStream()));
        this.outputStream = new ByteArrayOutputStream(socket.getOutputStream());
    }

    public ByteArrayInputStream getInputStream() {
        return inputStream;
    }

    public ByteArrayOutputStream getOutputStream() {
        return outputStream;
    }

    public byte[] read() throws IOException {
        int length = inputStream.readInteger(3);
        inputStream.skip(1); //sequence
        byte[] read = inputStream.read(length);
        return read;
    }

    public void write(Command command, int packetNumber) throws IOException {

        byte[] body = command.toByteArray();

        System.out.println("commond: "+ JSONUtil.toJsonStr(command)+""+ command.getClass());

        if(command instanceof AuthenticateCommand) {
            AuthenticateCommand command1 = (AuthenticateCommand) command;
//            System.out.println("salt"+command1.getSalt());
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeInteger(body.length, 3); // packet length
        buffer.writeInteger(packetNumber, 1);
        buffer.write(body, 0, body.length);
        outputStream.write(buffer.toByteArray());
        // though it has no effect in case of default (underlying) output stream (SocketOutputStream),
        // it may be necessary in case of non-default one
        outputStream.flush();
    }

    /**
     * @deprecated use {@link #write(Command, int)} instead
     */
    @Deprecated
    public void writeBuffered(Command command, int packetNumber) throws IOException {
        write(command, packetNumber);
    }

    public void write(Command command) throws IOException {
        write(command, 0);
    }


    @Override
    public boolean isOpen() {
        return !socket.isClosed();
    }

    @Override
    public void close() throws IOException {
        try {
            socket.shutdownInput(); // for socketInputStream.setEOF(true)
        } catch (Exception e) {
            // ignore
        }
        try {
            socket.shutdownOutput();
        } catch (Exception e) {
            // ignore
        }
        socket.close();
    }
}
