package com.ssc.cdc.CDCDataReplication.service;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.LogRecord;

public class CDCClient implements ICDCClient {

    private final DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;

    public CDCClient() {
        // Initialize and configure the Debezium engine to capture changes from PostgreSQL
        Configuration config = Configuration.create()
            .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
            .with("database.hostname", "your-postgresql-hostname")
            .with("database.port", "your-postgresql-port")
            .with("database.user", "your-postgresql-username")
            .with("database.password", "your-postgresql-password")
            .with("database.dbname", "your-postgresql-database-name")
            .with("database.server.name", "your-postgresql-server-name")
            .with("database.server.id", "1") // Unique identifier for the connector instance
            .with("schema.include.list", "your-schema-name") // Specify the schema to capture changes from
            .with("table.include.list", "your-schema-name.E02,your-schema-name.E45,your-schema-name.E55")
            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory") // For example, use FileDatabaseHistory
            .with("database.history.file.filename", "/path/to/dbhistory.dat") // File path to store connector history
            .build();

        this.debeziumEngine = EmbeddedEngine.create(Json.class)
            .using(config)
            .notifying(record -> {
                // Implementation to process captured change events
                String tableName = record.topic();
                String operation = record.topic();
                Map<String, Object> columnValues = (Map<String, Object>) record.value();

                // Process the change event as needed (e.g., store in an intermediate data structure, publish to Kafka, etc.)
                // You can use the LogRecord class or your custom Change class to represent the change event.
                LogRecord logRecord = new LogRecord(tableName, operation, columnValues);
                processChange(logRecord);
            })
            .using(this::handleError)
            .using(this::handleStop)
            .build();
    }

    @Override
    public List<LogRecord> readTransactionLogs() {
        // No need to implement this method since we are using the Debezium engine for capturing changes.
        // The captured changes will be processed directly in the constructor's notifying() implementation.
        return Collections.emptyList();
    }

    private void processChange(LogRecord logRecord) {
        // Implement your logic to process the captured change event.
        // You can store the change in an intermediate data structure, publish to Kafka, etc.
        // For example, you can have an intermediateDataStructure and store the changes in the `storeChangesInIntermediateDataStructure` method.
        // Or you can directly send the change to Kafka using KafkaTemplate.

        // Example:
        storeChangesInIntermediateDataStructure(logRecord);
    }

    private void storeChangesInIntermediateDataStructure(LogRecord logRecord) {
        // Store the captured change in the intermediate data structure (Map)
        String tableName = logRecord.getTableName();
        List<LogRecord> tableChanges = intermediateDataStructure.getOrDefault(tableName, new ArrayList<>());
        tableChanges.add(logRecord);
        intermediateDataStructure.put(tableName, tableChanges);
    }

    private void handleError(Throwable t) {
        // Implement your error handling logic here.
        // This method will be called when an error occurs during the change event processing.
    }

    private void handleStop(DebeziumEngine.ChangeEventSourceContext changeEventSourceContext) {
        // Implement your logic to handle the stop event (e.g., cleanup, close resources, etc.).
        // This method will be called when the Debezium engine is stopped.
    }
}

