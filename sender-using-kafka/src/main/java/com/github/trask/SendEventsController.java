package com.github.trask;

import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SendEventsController {

    private static final String CONNECTION_STRING = "Endpoint=****;"
            + "SharedAccessKeyName=****;"
            + "SharedAccessKey=****";

    private static final String HUB_NAME = "demohub";

    @RequestMapping("/send-events")
    public String sendEvents() throws Exception {

        Producer<String, String> producer = createProducer();

        Future<RecordMetadata> future1 = producer
                .send(new ProducerRecord<String, String>(HUB_NAME, "somedata", "Test Data 1"));
        Future<RecordMetadata> future2 = producer
                .send(new ProducerRecord<String, String>(HUB_NAME, "somedata", "Test Data 2"));
        Future<RecordMetadata> future3 = producer
                .send(new ProducerRecord<String, String>(HUB_NAME, "somedata", "Test Data 3"));

        future1.get();
        future2.get();
        future3.get();

        return "3 events sent!";
    }

    private static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(CLIENT_ID_CONFIG, "SenderUsingKafka");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("bootstrap.servers", "traskns.servicebus.windows.net:9093");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule"
                        + " required"
                        + " username=\"$ConnectionString\""
                        + " password=\"" + CONNECTION_STRING + "\";");
        return new KafkaProducer<>(properties);
    }
}
