package example;

import example.avro.User1;
import example.avro.User2;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class PulsarSchemaExample {

    // Before running this application, change the schema compatibility by the following command:
    //
    // ```bash
    // ./bin/pulsar-admin namespaces set-schema-compatibility-strategy -c FORWARD public/default
    // ```
    public static void main(String[] args) throws PulsarClientException {
        String topic = "my-avro-topic-0";
        @Cleanup PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        @Cleanup Consumer<User1> consumer = client.newConsumer(Schema.AVRO(User1.class))
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        @Cleanup Producer<User1> producer1 = client.newProducer(Schema.AVRO(User1.class))
                .topic(topic)
                .create();
        producer1.send(new User1("alice", 18));
        Message<User1> msg = consumer.receive(3, TimeUnit.SECONDS);
        assert msg != null;
        System.out.println("Received " + msg.getValue());

        @Cleanup final Producer<User2> producer2 = client.newProducer(Schema.AVRO(User2.class))
                .topic(topic)
                .create();
        producer2.send(new User2("bob", 20, 60));
        msg = consumer.receive(3, TimeUnit.SECONDS);
        assert msg != null;
        System.out.println("Received " + msg.getValue());
    }
}
