package schema;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class PulsarSchemaProducer {

    public static void main(String[] args) throws PulsarClientException {
        try (var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build()) {
            var id = client.newProducer(Schema.AVRO(User.class))
                    .topic("my-topic")
                    .create()
                    .newMessage()
                    .value(new User("xyz", 28))
                    .send();
            System.out.println("send to " + id);
        }
    }
}
