package org.example.deserializer;

import lombok.Data;

@Data
public class EventHeaderV4 implements EventHeader {
    // v1 (MySQL 3.23)
    private long timestamp;
    private EventType eventType;
    private long serverId;
    private long eventLength;
    // v3 (MySQL 4.0.2-4.1)
    private long nextPosition;
    private int flags;

    @Override
    public long getHeaderLength() {
        return 19;
    }

    @Override
    public long getDataLength() {
        return getEventLength() - getHeaderLength();
    }
}
