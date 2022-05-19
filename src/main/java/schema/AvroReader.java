package schema;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;

public class AvroReader<T> {

    private static final ThreadLocal<BinaryDecoder> decoder = new ThreadLocal<>();
    private final ReflectDatumReader<T> reader;

    public AvroReader(Schema schema) {
        this.reader = new ReflectDatumReader<>(schema);
    }

    public T read(byte[] bytes) throws IOException {
        return read(bytes, 0, bytes.length);
    }

    public T read(byte[] bytes, int start, int length) throws IOException {
        var decoderCache = AvroReader.decoder.get();
        var decoder = DecoderFactory.get().binaryDecoder(bytes, start, length, decoderCache);
        if (decoderCache == null) {
            AvroReader.decoder.set(decoder);
        }
        return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, start, length, decoder));
    }
}
