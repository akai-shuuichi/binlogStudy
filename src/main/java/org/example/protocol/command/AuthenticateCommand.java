
package org.example.protocol.command;

import lombok.Data;
import org.example.ClientCapabilities;
import org.example.io.ByteArrayOutputStream;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


@Data
public class AuthenticateCommand implements Command {

    private String schema;
    private String username;
    private String password;
    private String salt;
    private int clientCapabilities;
    private int collation;

    public AuthenticateCommand(String schema, String username, String password, String salt) {
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.salt = salt;
    }

    public void setCollation(int collation) {
        this.collation = collation;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int clientCapabilities = this.clientCapabilities;
        if (clientCapabilities == 0) {
            clientCapabilities = ClientCapabilities.LONG_FLAG |
                    ClientCapabilities.PROTOCOL_41 | ClientCapabilities.SECURE_CONNECTION;
            if (schema != null) {
                clientCapabilities |= ClientCapabilities.CONNECT_WITH_DB;
            }
        }
        buffer.writeInteger(clientCapabilities, 4);
        buffer.writeInteger(0, 4); // maximum packet length
        buffer.writeInteger(collation, 1);
        for (int i = 0; i < 23; i++) {
            buffer.write(0);
        }
        buffer.writeZeroTerminatedString(username);
        byte[] passwordSHA1 = "".equals(password) ? new byte[0] : passwordCompatibleWithMySQL411(password, salt);
        buffer.writeInteger(passwordSHA1.length, 1);
        buffer.write(passwordSHA1);
        if (schema != null) {
            buffer.writeZeroTerminatedString(schema);
        }
        return buffer.toByteArray();
    }

    /**
     * see mysql/sql/password.c scramble(...)
     */
    public static byte[] passwordCompatibleWithMySQL411(String password, String salt) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] passwordHash = sha.digest(password.getBytes());
        return xor(passwordHash, sha.digest(union(salt.getBytes(), sha.digest(passwordHash))));
    }

    private static byte[] union(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] r = new byte[a.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = (byte) (a[i] ^ b[i]);
        }
        return r;
    }

}
