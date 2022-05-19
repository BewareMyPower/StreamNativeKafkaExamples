package schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

public class AvroWriter<T> {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final BinaryEncoder encoder;
    private final ReflectDatumWriter<T> writer;

    public AvroWriter(final Schema schema) {
        this.encoder = EncoderFactory.get().binaryEncoder(out, null);
        // TODO: addLogicalTypeConversion for ReflectData
        this.writer = new ReflectDatumWriter<>(schema, new ReflectData());
    }

    public synchronized void write(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    public synchronized byte[] write(T value) throws IOException {
        byte[] bytes = null;
        try {
            writer.write(value, encoder);
        } finally {
            try {
                encoder.flush();
                bytes = out.toByteArray();
            } finally {
                out.reset();
            }
        }
        return bytes;
    }
}
