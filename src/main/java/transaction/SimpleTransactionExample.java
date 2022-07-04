package transaction;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SimpleTransactionExample {

    private static final String TOPIC = "my-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TransactionUtils.createTopic(TOPIC, 2);
        @Cleanup var producer = TransactionUtils.createProducer("my-txn");
        producer.beginTransaction();

        var metadata = producer.send(new ProducerRecord<>(TOPIC, 0, null, "M1")).get();
        System.out.println("sent M1 to " + metadata);
        metadata = producer.send(new ProducerRecord<>(TOPIC, 1, null, "M2")).get();
        System.out.println("sent M2 to " + metadata);

        producer.commitTransaction();

        producer.beginTransaction();
        metadata = producer.send(new ProducerRecord<>(TOPIC, 0, null, "M3")).get();
        System.out.println("sent M3 to " + metadata);

        consumeMessages("group1", true);
        System.out.println("----------------");
        consumeMessages("group2", false);
    }

    private static void consumeMessages(String group, boolean readCommitted) {
        @Cleanup final var consumer = TransactionUtils.createConsumer(TOPIC, readCommitted);
        consumer.subscribe(Collections.singleton(TOPIC));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(1500));
            if (records.isEmpty()) {
                break;
            }
            records.forEach(record -> System.out.println("Received " + record.value() + " from "
                    + record.topic() + "-" + record.partition() + "@" + record.offset()));
        }
    }
}
