package example;

import example.avro.User1;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaAvroConsumerExample {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8001");

        @Cleanup final KafkaConsumer<String, User1> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("my-avro-kafka-topic"));
        while (true) {
            ConsumerRecords<String, User1> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> System.out.println("Received " + record.value() + " from "
                    + record.topic() + "-" + record.partition() + "@" + record.offset()));
        }
    }
}
