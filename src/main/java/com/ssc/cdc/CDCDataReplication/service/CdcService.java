package com.ssc.cdc.CDCDataReplication.service;

import com.nimbusds.jose.shaded.gson.Gson;
import com.ssc.cdc.CDCDataReplication.model.E02;
import jakarta.persistence.EntityManager;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.event.spi.PostUpdateEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.HashMap;
import java.util.Map;

@Service
public class CdcService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EntityManager entityManager;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleDb2Event(Object event) {
        if (event instanceof PostInsertEvent) {
            // Handle insert event
            Object entity = ((PostInsertEvent) event).getEntity();
            if (entity instanceof E02 || entity instanceof E04 || entity instanceof E09) {
                String json = convertToJson(entity);
                sendToKafka(json);
            }
        } else if (event instanceof PostUpdateEvent) {
            // Handle update event
            PostUpdateEvent updateEvent = (PostUpdateEvent) event;
            Object entity = updateEvent.getEntity();
            if (entity instanceof E02 || entity instanceof E04 || entity instanceof E09) {
                // Get the old and new state of the entity
                Object[] oldState = updateEvent.getOldState();
                Object[] newState = updateEvent.getState();

                // Compare old and new state to find changes
                Map<String, Object> changes = new HashMap<>();
                for (int i = 0; i < oldState.length; i++) {
                    if (oldState[i] == null || !oldState[i].equals(newState[i])) {
                        // Field value has changed
                        String fieldName = updateEvent.getPersister().getPropertyNames()[i];
                        changes.put(fieldName, newState[i]);
                    }
                }

                // Only send to Kafka if there are changes
                if (!changes.isEmpty()) {
                    String json = convertToJson(changes);
                    sendToKafka(json);
                }
            }
        } else if (event instanceof PostDeleteEvent) {
            // Handle delete event
            // Similar to insert and update, you need to check the entity type and send to Kafka
            // Handle delete event
            PostDeleteEvent deleteEvent = (PostDeleteEvent) event;
            Object entity = deleteEvent.getEntity();
            if (entity instanceof E02 || entity instanceof E04 || entity instanceof E09) {
                // Get the state of the entity before the delete
                Object[] oldState = deleteEvent.getDeletedState();

                // Convert the old state to JSON format
                String json = convertToJson(oldState);
                sendToKafka(json);
            }

        }
    }

    private String convertToJson(Object entity) {
        // Convert the entity to JSON format
        return new Gson().toJson(entity);
    }

    private void sendToKafka(String json) {
        // Send JSON data to Kafka topic
        kafkaTemplate.send("<kafka_topic>", json);
    }
}
