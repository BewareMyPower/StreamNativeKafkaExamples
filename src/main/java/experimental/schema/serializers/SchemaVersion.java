package experimental.schema.serializers;

import java.nio.ByteBuffer;

// Migrate from org.apache.pulsar.client.impl.schema.LongSchema without some checks.
public class SchemaVersion {

    public static byte[] encode(long version) {
        return new byte[] {
                (byte) (version >>> 56),
                (byte) (version >>> 48),
                (byte) (version >>> 40),
                (byte) (version >>> 32),
                (byte) (version >>> 24),
                (byte) (version >>> 16),
                (byte) (version >>> 8),
                (byte) version
        };
    }

    public static long decode(ByteBuffer buffer) {
        long value = 0L;
        for (int i = 0; i < 8; i++) {
            value <<= 8;
            value |= buffer.get() & 0xFF;
        }
        return value;
    }

}
