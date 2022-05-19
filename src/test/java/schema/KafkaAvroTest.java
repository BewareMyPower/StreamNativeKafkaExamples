package schema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.testng.Assert;
import org.testng.annotations.Test;
import schema.serializers.KafkaAvroConfigs;
import schema.serializers.KafkaAvroDeserializer;
import schema.serializers.KafkaAvroSerializer;

@Slf4j
public class KafkaAvroTest {

    @AllArgsConstructor
    @EqualsAndHashCode
    @NoArgsConstructor
    @ToString
    static class V1Data {
        int i;
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @NoArgsConstructor
    @ToString
    static class V2Data {
        int i;
        String s;
    }

    @Test
    public void testForwardCompatibility() throws Exception {
        final String topic = "test-forward-compatibility-" + System.currentTimeMillis();
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
        admin.namespaces().setSchemaCompatibilityStrategy(
                "public/default", SchemaCompatibilityStrategy.FORWARD);
        // Now it's allowed to evolve schema from V1Data to V2Data

        final KafkaProducer<String, V1Data> producer1 = new KafkaProducer<>(newAvroProducerProps());
        @Cleanup
        final KafkaConsumer<String, V1Data> consumer1 = new KafkaConsumer<>(newAvroConsumerProps("group"));
        producer1.send(new ProducerRecord<>(topic, new V1Data(1))).get();
        final List<Object> valueList = new ArrayList<>();
        receiveUntil(consumer1, topic, 1, record -> {
            valueList.add(record.value());
            log.info("Received " + record.value() + " from " + record.offset());
        });
        Assert.assertEquals(valueList, Collections.singletonList(new V1Data(1)));
        validateSchema(admin, topic, "{\"type\":\"record\",\"name\":\"V1Data\",\"namespace\":"
                + "\"schema.KafkaAvroTest\",\"fields\":[{\"name\":\"i\",\"type\":\"int\"}]}", 0);

        final KafkaProducer<String, V2Data> producer2 = new KafkaProducer<>(newAvroProducerProps());
        producer2.send(new ProducerRecord<>(topic, new V2Data(2, "A")));
        producer2.send(new ProducerRecord<>(topic, new V2Data(3, "B")));
        validateSchema(admin, topic, "{\"type\":\"record\",\"name\":\"V2Data\",\"namespace\":"
                + "\"schema.KafkaAvroTest\",\"fields\":[{\"name\":\"i\",\"type\":\"int\"},{\"name\":\"s\","
                + "\"type\":[\"null\",\"string\"],\"default\":null}]}", 1);

        valueList.clear();
        receiveUntil(consumer1, topic, 2, record -> {
            valueList.add(record.value());
            log.info("Received " + record.value() + " from " + record.offset());
        });
        Assert.assertEquals(valueList, Arrays.asList(new V2Data(2, "A"), new V2Data(3, "B")));

        // TODO: should we allow the existing producer1 to send more messages?

        final KafkaProducer<String, V1Data> producer3 = new KafkaProducer<>(newAvroProducerProps());
        try {
            producer3.send(new ProducerRecord<>(topic, new V1Data(4))).get();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SerializationException);
            Assert.assertTrue(e.getMessage().contains("incompatible"));
        }


        producer1.close();
        producer2.close();
        consumer1.close();
    }

    private void validateSchema(final PulsarAdmin admin, final String topic, final String schema, final long version)
            throws PulsarAdminException {
        final SchemaInfoWithVersion schemaInfo = admin.schemas().getSchemaInfoWithVersion(topic);
        log.info("[{}] Current schema: {}, version: {}", topic,
                schemaInfo.getSchemaInfo().getSchemaDefinition(), schemaInfo.getVersion());
        Assert.assertEquals(schemaInfo.getSchemaInfo().getSchemaDefinition(), schema);
        Assert.assertEquals(schemaInfo.getVersion(), version);
    }

    private static <K, V> void receiveUntil(final KafkaConsumer<K, V> consumer,
                                            final String topic,
                                            final int numMessages,
                                            final Consumer<ConsumerRecord<K, V>> callback) {
        consumer.subscribe(Collections.singleton(topic));
        int n = 0;
        while (n < numMessages) {
            for (ConsumerRecord<K, V> record : consumer.poll(Duration.ofSeconds(1))) {
                callback.accept(record);
                n++;
            }
        }
    }

    private static Properties newAvroProducerProps() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroConfigs.SCHEMA_REGISTRY_URL, "http://localhost:8080");
        return props;
    }

    private static Properties newAvroConsumerProps(final String group) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroConfigs.SCHEMA_REGISTRY_URL, "http://localhost:8080");
        return props;
    }
}
