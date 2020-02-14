package com.github.trask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventListener {

    private static final String HUB_NAME = "demohub";
    
    @KafkaListener(topics = HUB_NAME)
    public void receiveEvent(ConsumerRecord<String, String> event) {
        System.out.println("received: " + event);
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("select 1");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
