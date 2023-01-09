package experimental.schema;

import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

public class PulsarSchemaConsumer {

    public static void main(String[] args) throws PulsarClientException {
        try (PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build()) {
            @Cleanup Consumer<User> consumer = client.newConsumer(Schema.AVRO(User.class))
                    .topic("my-topic")
                    .subscriptionName("sub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            while (true) {
                final Message<User> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                System.out.println("received " + msg.getValue() + " from " + msg.getMessageId());
            }
        }
    }
}
