package experimental;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

public class OAuthProducerExample {

    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("sasl.login.callback.handler.class",
                io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.jaas.config",
                String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule\n"
                        + "required oauth.issuer.url=\"%s\"\n"
                        + "oauth.credentials.url=\"%s\";",
                        "http://localhost:4444", "file:///tmp/admin_credentials.json"));

        final String topic = "my-topic";

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            final RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, "value")).get();
            System.out.println("send to " + metadata);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
