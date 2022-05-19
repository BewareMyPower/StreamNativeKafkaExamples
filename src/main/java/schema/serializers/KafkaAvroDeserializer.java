package schema.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import schema.AvroReader;

public class KafkaAvroDeserializer implements Deserializer<Object> {

    // TODO: limit the max reader cache size
    private final Map<Schema, AvroReader<Object>> readerCache = Collections.synchronizedMap(new IdentityHashMap<>());
    private CachedSchemaRegistryClient schemaRegistry;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (isKey) {
            throw new IllegalArgumentException("KafkaAvroDeserializer cannot be used for key");
        }
        this.schemaRegistry = new CachedSchemaRegistryClient(KafkaAvroConfigs.getSchemaRegistryUrl(configs));
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final Long version = getSchemaVersion(buffer);
        final Schema schema = schemaRegistry.getSchema(topic, version);
        try {
            return readerCache.computeIfAbsent(schema, __ -> new AvroReader<>(schema))
                    .read(bytes, KafkaAvroConfigs.HEADER_SIZE, bytes.length - KafkaAvroConfigs.HEADER_SIZE);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    private Long getSchemaVersion(final ByteBuffer buffer) {
        if (buffer.remaining() < KafkaAvroConfigs.HEADER_SIZE) {
            return null;
        }
        buffer.mark();
        final byte[] marker = new byte[2];
        buffer.get(marker);
        if (!Arrays.equals(marker, KafkaAvroConfigs.MARKER)) {
            buffer.reset();
            return null;
        }
        return SchemaVersion.decode(buffer);
    }
}
