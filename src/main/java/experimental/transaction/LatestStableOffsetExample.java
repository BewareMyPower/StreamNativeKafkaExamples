package experimental.transaction;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

public class LatestStableOffsetExample {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String topic = "my-topic-lso";
        TransactionUtils.deleteTopic(topic);

        @Cleanup KafkaProducer<String, String> producer0 = TransactionUtils.createProducer("txn-1");
        producer0.beginTransaction();
        RecordMetadata metadata = TransactionUtils.send(producer0, topic, "P0-M0");
        System.out.println("sent P0-M0 to " + metadata);
        @Cleanup KafkaProducer<String, String> producer1 = TransactionUtils.createProducer("txn-2");
        producer1.beginTransaction();
        metadata = TransactionUtils.send(producer1, topic, "P1-M0");
        System.out.println("sent P1-M0 to " + metadata);
        producer0.commitTransaction();

        producer0.beginTransaction();
        metadata = TransactionUtils.send(producer0, topic, "P0-M1");
        System.out.println("sent P0-M1 to " + metadata);
        producer0.commitTransaction();

        // message order: P0-M0, P1-M1, P0-COMMIT, P0-M1, P0-COMMIT
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        @Cleanup AdminClient admin = TransactionUtils.createAdmin();
        ListOffsetsResult listOffsetsResult = admin.listOffsets(
                Collections.singletonMap(topicPartition, OffsetSpec.latest()));
        ListOffsetsResult.ListOffsetsResultInfo info = listOffsetsResult.partitionResult(topicPartition).get();
        System.out.println("list offset (uncommitted): " + info);

        listOffsetsResult = admin.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest()),
                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED));
        info = listOffsetsResult.partitionResult(topicPartition).get();
        System.out.println("list offset (committed): " + info);
    }
}
