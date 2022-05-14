package schema;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

public class PulsarSchemaConsumer {

    public static void main(String[] args) throws PulsarClientException {
        try (var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build()) {
            var consumer = client.newConsumer(Schema.AVRO(User.class))
                    .topic("my-topic")
                    .subscriptionName("sub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            while (true) {
                var msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                System.out.println("received " + msg.getValue() + " from " + msg.getMessageId());
            }
        }
    }
}
