package schema;

import org.testng.Assert;
import org.testng.annotations.Test;
import schema.serializers.SchemaVersion;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class SchemaVersionTest {

    private static final long VALUE = 1234578L;
    private static final byte[] DEFAULT_VALUE_BYTES = { 0, 0, 0, 0, 0, 18, -42, -110 };

    @Test
    public void testEncodeDecode() {
        Assert.assertEquals(SchemaVersion.encode(VALUE), DEFAULT_VALUE_BYTES);
        final ByteBuffer buffer = ByteBuffer.wrap(DEFAULT_VALUE_BYTES);
        Assert.assertEquals(SchemaVersion.decode(buffer), VALUE);
        Assert.assertEquals(buffer.remaining(), 0);

        for (int i = 0; i < 100; i++) {
            final ByteBuffer innerBuffer = ByteBuffer.wrap(SchemaVersion.encode(VALUE + i));
            Assert.assertEquals(SchemaVersion.decode(innerBuffer), VALUE + i);
            Assert.assertEquals(innerBuffer.remaining(), 0);
        }
    }

    @Test
    public void testInvalidInput() {
        Assert.assertThrows(NullPointerException.class, () -> SchemaVersion.decode(null));
        Assert.assertThrows(BufferUnderflowException.class, () -> SchemaVersion.decode(ByteBuffer.wrap(new byte[2])));
    }
}
