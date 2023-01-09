package example;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import example.avro.User1;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.Cleanup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAvroProducerExample {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8001");

        final String topic = "my-kafka-avro-topic";
        @Cleanup final KafkaProducer<String, User1> producer = new KafkaProducer<>(props);
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, new User1("alice", 10))).get();
        System.out.println("Sent to " + metadata);
    }
}
