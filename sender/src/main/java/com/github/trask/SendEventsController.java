package com.github.trask;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;

@RestController
public class SendEventsController {

    private static final String CONNECTION_STRING = "Endpoint=****;"
            + "SharedAccessKeyName=****;"
            + "SharedAccessKey=****";

    private static final String HUB_NAME = "demohub";

    @RequestMapping("/send-events")
    public String sendEvents() {
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(CONNECTION_STRING, HUB_NAME)
                .buildProducerClient();

        final byte[] body = "EventData Sample 1".getBytes(UTF_8);
        final byte[] body2 = "EventData Sample 2".getBytes(UTF_8);
        final byte[] body3 = "EventData Sample 3".getBytes(UTF_8);

        EventDataBatch eventDataBatch = producer.createBatch();
        eventDataBatch.tryAdd(new EventData(body));
        eventDataBatch.tryAdd(new EventData(body2));
        eventDataBatch.tryAdd(new EventData(body3));

        producer.send(eventDataBatch);

        return "3 events sent!";
    }
}
