package experimental.transaction;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class TransactionUtils {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static boolean createTopic(final String topic, final int numPartitions) {
        @Cleanup final AdminClient admin = createAdmin();
        try {
            admin.createTopics(Collections.singleton(new NewTopic(topic, numPartitions, (short) 1))).all().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Failed to create {}: {}", topic, e.getMessage());
            return false;
        }
    }

    public static void deleteTopic(final String topic) {
        @Cleanup final AdminClient admin = createAdmin();
        try {
            admin.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Failed to delete {}: {}", topic, e.getMessage());
        }
    }

    public static AdminClient createAdmin() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return AdminClient.create(props);
    }

    public static KafkaProducer<String, String> createProducer(@Nullable final String txnId) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (txnId != null) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
            final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            producer.initTransactions();
            return producer;
        } else {
            return new KafkaProducer<>(props);
        }
    }

    public static RecordMetadata send(final KafkaProducer<String, String> producer,
                                      final String topic,
                                      final String value) throws ExecutionException, InterruptedException {
        return send(producer, topic, 0, value);
    }

    public static RecordMetadata send(final KafkaProducer<String, String> producer,
                                      final String topic,
                                      final int partition,
                                      final String value) throws ExecutionException, InterruptedException {
        return producer.send(new ProducerRecord<>(topic, partition, null, value)).get();
    }

    public static KafkaConsumer<String, String> createConsumer(final String topic) {
        return createConsumer(topic, true);
    }

    public static KafkaConsumer<String, String> createConsumer(final String topic, boolean readCommitted) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        } else {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        }
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }
}
