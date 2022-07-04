package transaction;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class InterleavingTransactionExample {

    private static final String TOPIC = "my-test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TransactionUtils.createTopic(TOPIC, 2);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final var future1 = executor.submit(() -> {
            @Cleanup final var producer1 = TransactionUtils.createProducer("X1");
            @Cleanup final var producer2 = TransactionUtils.createProducer("X2");
            @Cleanup final var producer3 = TransactionUtils.createProducer(null);

            try {
                producer1.beginTransaction();
                producer1.send(new ProducerRecord<>(TOPIC, 0, null, "X1: M1-p0")).get();
                producer2.beginTransaction();
                producer2.send(new ProducerRecord<>(TOPIC, 0, null, "X2: M1-p0")).get();
                producer1.send(new ProducerRecord<>(TOPIC, 0, null, "X1: M2-p0")).get();
                producer1.send(new ProducerRecord<>(TOPIC, 0, null, "X1: M3-p0")).get();
                producer2.send(new ProducerRecord<>(TOPIC, 1, null, "X2: M2-p1")).get();
                producer1.send(new ProducerRecord<>(TOPIC, 1, null, "X1: M4-p1")).get();
                producer3.send(new ProducerRecord<>(TOPIC, 0, null, "Mx-p0")).get();
                producer2.send(new ProducerRecord<>(TOPIC, 1, null, "X2: M3-p1")).get();
                producer3.send(new ProducerRecord<>(TOPIC, 1, null, "My-p1")).get();
                producer2.send(new ProducerRecord<>(TOPIC, 0, null, "X2: M4-p0")).get();
                producer2.commitTransaction();
                producer1.send(new ProducerRecord<>(TOPIC, 0, null, "X1: M5-p0")).get();
                producer1.send(new ProducerRecord<>(TOPIC, 1, null, "X1: M6-p1")).get();
                producer1.commitTransaction();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        final var future2 = executor.submit(() -> {
            @Cleanup final var consumer = TransactionUtils.createConsumer(TOPIC);
            for (int i = 0; i < 12; ) {
                final var records = consumer.poll(Duration.ofSeconds(1));
                i += records.count();
                records.forEach(record -> log.info("Received {} from {}-{}@{}",
                        record.value(), record.topic(), record.partition(), record.offset()));
            }
        });
        future1.get();
        future2.get();
        executor.shutdown();
        //  Received X1: M1-p0 from test-topic-0@0
        //  Received X2: M1-p0 from test-topic-0@1
        //  Received X1: M2-p0 from test-topic-0@2
        //  Received X1: M3-p0 from test-topic-0@3
        //  Received Mx-p0 from test-topic-0@4
        //  Received X2: M4-p0 from test-topic-0@5
        //  Received X1: M5-p0 from test-topic-0@6
        //  Received X2: M2-p1 from test-topic-1@0
        //  Received X1: M4-p1 from test-topic-1@1
        //  Received X2: M3-p1 from test-topic-1@2
        //  Received My-p1 from test-topic-1@3
        //  Received X1: M6-p1 from test-topic-1@5
    }
}
