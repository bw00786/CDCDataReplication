package com.ssc.cdc.CDCDataReplication.service;

import aj.org.objectweb.asm.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

@Service
public class KafkaConsumerService {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private E02Repository e02Repository;

    @Autowired
    private E04Repository e04Repository;

    @Autowired
    private E09Repository e09Repository;

    @KafkaListener(topics = "<kafka_topic>", groupId = "<group_id>")
    public void receiveFromKafka(String message) {
        try {
            // Deserialize JSON data to a generic Map
            Map<String, Object> dataMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            // Extract the entity type from the JSON data
            String entityType = dataMap.get("entityType").toString();

            // Remove the entityType key from the dataMap to get the entity data
            dataMap.remove("entityType");

            // Determine the appropriate entity class based on the entityType
            Object entity;
            switch (entityType) {
                case "E02":
                    entity = objectMapper.convertValue(dataMap, E02.class);
                    e02Repository.save((E02) entity);
                    break;
                case "E04":
                    entity = objectMapper.convertValue(dataMap, E04.class);
                    e04Repository.save((E04) entity);
                    break;
                case "E09":
                    entity = objectMapper.convertValue(dataMap, E09.class);
                    e09Repository.save((E09) entity);
                    break;
                default:
                    // Handle unsupported entity types or log an error
            }
        } catch (IOException e) {
            // Handle deserialization errors or log an error
        }
    }
}
