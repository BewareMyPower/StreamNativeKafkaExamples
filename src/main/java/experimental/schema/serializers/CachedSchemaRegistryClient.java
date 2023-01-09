package experimental.schema.serializers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
public class CachedSchemaRegistryClient {

    // TODO: add an identifier for better logs

    // topic -> { schema -> schema version }
    private final Map<String, Map<Schema, Long>> versionCache = new ConcurrentHashMap<>();
    // topic -> { schema version -> schema }
    private final Map<String, Map<Long, Schema>> schemaCache = new ConcurrentHashMap<>();
    private final PulsarAdmin admin;

    public CachedSchemaRegistryClient(final String schemaRegistryUrl) {
        try {
            admin = PulsarAdmin.builder().serviceHttpUrl(schemaRegistryUrl).build();
        } catch (PulsarClientException e) {
            throw new IllegalStateException(e);
        }
    }

    public long tryAddSchema(final String topic, final Schema schema) {
        synchronized (versionCache) {
            final Map<Schema, Long> versionMap = versionCache.computeIfAbsent(topic, __ -> new IdentityHashMap<>());
            if (versionMap.containsKey(schema)) {
                return versionMap.get(schema);
            } else {
                try {
                    final String currentSchemaJson = schema.toString();
                    // NOTE: createSchema won't fail even if the schema failed to update
                    admin.schemas().createSchema(topic, SchemaInfo.builder().name("")
                            .schema(currentSchemaJson.getBytes(StandardCharsets.UTF_8))
                            .type(SchemaType.AVRO)
                            .build());
                    final SchemaInfoWithVersion schemaInfoWithVersion =
                            admin.schemas().getSchemaInfoWithVersion(topic);
                    final String latestSchemaJson = schemaInfoWithVersion.getSchemaInfo().getSchemaDefinition();
                    final long schemaVersion = schemaInfoWithVersion.getVersion();
                    if (currentSchemaJson.equals(latestSchemaJson)) {
                        log.info("[{}] Add schema successfully with version: {}", topic, schemaVersion);
                        versionMap.put(schema, schemaVersion);
                        return schemaVersion;
                    } else {
                        log.warn("[{}] The current schema {} is incompatible with the latest schema {}",
                                topic, currentSchemaJson, latestSchemaJson);
                        throw new SerializationException("The current schema " + currentSchemaJson
                                + " is incompatible with the latest schema " + latestSchemaJson);
                    }
                } catch (PulsarAdminException e) {
                    log.error("Failed to get schema for topic {}", topic, e);
                    throw new SerializationException(e);
                }
            }
        }
    }

    public Schema getSchema(final String topic, final Long schemaVersion) {
        return schemaCache.computeIfAbsent(topic, __ -> Collections.synchronizedMap(new HashMap<>()))
                .computeIfAbsent(schemaVersion, __ -> {
                    log.info("Try to get schema of topic {} and version {}", topic, schemaVersion);
                    try {
                        final SchemaInfo schemaInfo = getSchemaInfo(topic, schemaVersion);
                        if (schemaInfo != null) {
                            log.info("Load schema reader for version({}), schema is : {}, schemaInfo: {}",
                                    schemaVersion, schemaInfo.getSchemaDefinition(), schemaInfo);
                            final Schema.Parser parser = new Schema.Parser();
                            parser.setValidateDefaults(false);
                            return parser.parse(schemaInfo.getSchemaDefinition());
                        } else {
                            throw new SerializationException("Failed to get schema info for topic " + topic);
                        }
                    } catch (PulsarAdminException e) {
                        throw new SerializationException(e);
                    }
                });
    }

    private SchemaInfo getSchemaInfo(final String topic, final Long schemaVersion) throws PulsarAdminException {
        if (schemaVersion != null) {
            final SchemaInfo schemaInfo = admin.schemas().getSchemaInfo(topic, schemaVersion);
            if (schemaInfo != null) {
                return schemaInfo;
            } else {
                log.warn("No schema for topic {} with version {}, try to get latest schema", topic, schemaVersion);
                return admin.schemas().getSchemaInfo(topic);
            }
        } else {
            return admin.schemas().getSchemaInfo(topic);
        }
    }
}
