package experimental.schema.serializers;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.common.serialization.Serializer;
import experimental.schema.AvroWriter;

public class KafkaAvroSerializer implements Serializer<Object> {

    private CachedSchemaRegistryClient schemaRegistry;
    private SpecificData specificData;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (isKey) {
            throw new IllegalArgumentException("KafkaAvroSerializer cannot be used for key");
        }
        this.schemaRegistry = new CachedSchemaRegistryClient(KafkaAvroConfigs.getSchemaRegistryUrl(configs));
        this.specificData = KafkaAvroConfigs.getSpecificData(configs);
    }

    @Override
    public byte[] serialize(String topic, Object value) {
        final Schema schema = specificData.getSchema(value.getClass());
        try {
            final long schemaVersion = schemaRegistry.tryAddSchema(topic, schema);
            final AvroWriter<Object> writer = new AvroWriter<>(schema);
            writer.write(KafkaAvroConfigs.MARKER);
            writer.write(SchemaVersion.encode(schemaVersion));
            return writer.write(value);
        } catch (IOException e) {
            // TODO: differ the PulsarAdminException and other exceptions
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // No ops
    }

}
