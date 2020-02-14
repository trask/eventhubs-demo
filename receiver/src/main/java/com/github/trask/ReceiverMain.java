package com.github.trask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;

@SpringBootApplication
public class ReceiverMain {

    private static final String CONNECTION_STRING = "Endpoint=****;"
            + "SharedAccessKeyName=****;"
            + "SharedAccessKey=****";

    private static final String HUB_NAME = "demohub";

    public static void main(String[] args) {
        EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
                .connectionString(CONNECTION_STRING, HUB_NAME)
                .processEvent(new EventConsumer())
                .processError(new ErrorConsumer())
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .checkpointStore(new InMemoryCheckpointStore())
                .buildEventProcessorClient();

        eventProcessorClient.start();

        System.out.println("waiting for events...");
    }

    private static class EventConsumer implements Consumer<EventContext> {

        @Override
        public void accept(EventContext t) {
            System.out.println("received 1 event!");
            try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("select 1");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static class ErrorConsumer implements Consumer<ErrorContext> {

        @Override
        public void accept(ErrorContext t) {
            System.out.println("received 1 error!");
            t.getThrowable().printStackTrace();
        }
    }
}
