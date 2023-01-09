package experimental.schema.serializers;

import java.util.Map;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

public class KafkaAvroConfigs {

    public static final byte[] MARKER = { 0x04, 0x03 };
    public static final int HEADER_SIZE = MARKER.length + 8;
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String ALLOW_NULL = "allow.null";

    public static String getSchemaRegistryUrl(Map<String, ?> configs) {
        final String schemaRegistryUrl = (String) configs.get(SCHEMA_REGISTRY_URL);
        if (schemaRegistryUrl == null) {
            throw new IllegalArgumentException("No key for " + SCHEMA_REGISTRY_URL);
        }
        return schemaRegistryUrl;
    }

    public static SpecificData getSpecificData(Map<String, ?> configs) {
        Boolean allowNull = (Boolean) configs.get(ALLOW_NULL);
        if (allowNull == null) {
            allowNull = true;
        }
        if (allowNull) {
            return new ReflectData.AllowNull();
        } else {
            return new ReflectData();
        }
    }
}
