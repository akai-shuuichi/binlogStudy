
package org.example;

public enum ChecksumType {

    NONE(0), CRC32(4);

    private int length;

    private ChecksumType(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    private static final ChecksumType[] VALUES = values();

    public static ChecksumType byOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

}
