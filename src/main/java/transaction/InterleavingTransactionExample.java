package transaction;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InterleavingTransactionExample {

    private static final String TOPIC = "my-test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TransactionUtils.deleteTopic(TOPIC);
        final var maxRetryCount = 10;
        for (int i = 0; i < maxRetryCount; i++) {
            if (TransactionUtils.createTopic(TOPIC, 2)) {
                break;
            }
            if (i == maxRetryCount - 1) {
                throw new RuntimeException("Cannot create topic " + TOPIC);
            }
            Thread.sleep(100); // wait until the topic is actually deleted
        }
        @Cleanup final var producer1 = TransactionUtils.createProducer("X1");
        @Cleanup final var producer2 = TransactionUtils.createProducer("X2");
        @Cleanup final var producer3 = TransactionUtils.createProducer(null);

        try {
            producer1.beginTransaction();
            TransactionUtils.send(producer1, TOPIC, 0, "X1: M1-p0");
            producer2.beginTransaction();
            TransactionUtils.send(producer2, TOPIC, 0, "X2: M1-p0");
            TransactionUtils.send(producer1, TOPIC, 0, "X1: M2-p0");
            TransactionUtils.send(producer1, TOPIC, 0, "X1: M3-p0");
            TransactionUtils.send(producer2, TOPIC, 1, "X2: M2-p1");
            TransactionUtils.send(producer1, TOPIC, 1, "X1: M4-p1");
            TransactionUtils.send(producer3, TOPIC, 0, "Mx-p0");
            TransactionUtils.send(producer2, TOPIC, 1, "X2: M3-p1");
            TransactionUtils.send(producer3, TOPIC, 1, "My-p1");
            TransactionUtils.send(producer2, TOPIC, 0, "X2: M4-p0");
            producer2.commitTransaction();
            TransactionUtils.send(producer1, TOPIC, 0, "X1: M5-p0");
            TransactionUtils.send(producer1, TOPIC, 1, "X1: M6-p1");
            producer1.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        @Cleanup final var consumer = TransactionUtils.createConsumer(TOPIC);
        for (int i = 0; i < 12; ) {
            final var records = consumer.poll(Duration.ofSeconds(1));
            i += records.count();
            records.forEach(record -> System.out.println("Received " + record.value() + " from " + record.topic()
                    + "-" + record.partition() + "@" + record.offset()));
        }

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
