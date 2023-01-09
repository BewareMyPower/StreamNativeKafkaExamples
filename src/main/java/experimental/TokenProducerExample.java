package experimental;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

public class TokenProducerExample {

    public static void main(String[] args) throws InterruptedException {
        final var token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpoeWRyYS5qd3QuYWNjZXNzLXRva2VuIiwidHlwIjoiSldUIn0.eyJh"
                + "dWQiOltdLCJjbGllbnRfaWQiOiJhZG1pbiIsImV4cCI6MTY1MjQzMjIxMCwiZXh0Ijp7fSwiaWF0IjoxNjUyNDI4NjEwLCJpc3Mi"
                + "OiJodHRwOi8vMTI3LjAuMC4xOjQ0NDQvIiwianRpIjoiMjIxYWRmZmYtZjljZS00NTQ4LThkNGQtOTgyMzkwZWU2ZDdiIiwibmJm"
                + "IjoxNjUyNDI4NjEwLCJzY3AiOltdLCJzdWIiOiJhZG1pbiJ9.OBgThJ1AUP29SYEn0u5GraWTlw_c_veNL0eamP8-QR4G9auACFM"
                + "bHXEqqvVfOb3ZPUvLgcvEGgicNtEqyBg134XPcxlDPLw9EKvQGt6miwN0gJUVhNlnBF0EZh5z9yQRjbZIGWfiWg-0i_dG1iAR-fZ"
                + "QWKDjqUbCZjMLaSH-h2QjlOXSIEKTzsxuxdI_mKT9H4oQkCpuYWk4NUmVRszpj9Ukz4LcBtDKRciI-Xzq2CovzwyQr8TsMyg7Nb8"
                + "QCBMqZgTRyMvCzBwi5Mn4ycChU4WzMhh9LdTjHlWAmciXDcQEddRGDLJgYYBJRAOELenK4KQF0eLtCNTXLK7rrtKsxPfoj8se6z7"
                + "TdFAmdVvxwjclxQDqWn8LpTRK4y30JTgx9dA1WHO88mycn4WN8BSCjonEFcnfyWnEfMnQGnScB3hmVOwy9pDIChjqwcxoJO4sMbT"
                + "nNJLEF1qyrQSnkEfbYAD85pF7Cp4AOgAagdwk3bcevPVCpBm8RLPQ69ARAtgLDQl7Rs5dfb9j32L9W7qhM-81Vf5noEvKZ3i38mg"
                + "Wej9a_HDMo-Vt-raSqFlmqjqw5zScVZ8ty_-HN-ZH4lSeETO2rMam1E1yUN0RH6LefypxWSsWr8sjPlO8HLxZ4Cs-0zMrORG2ovs"
                + "Ubu2qFerXR6jQ85pT2RE6w_jlj-dZvII";

        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("""
                org.apache.kafka.common.security.plain.PlainLoginModule
                required username="%s" password="token:%s";
                """, "public/default", token));

        final var topic = "my-topic";

        try (final var producer = new KafkaProducer<String, String>(props)) {
            final var metadata = producer.send(new ProducerRecord<>(topic, "value")).get();
            System.out.println("send to " + metadata);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
