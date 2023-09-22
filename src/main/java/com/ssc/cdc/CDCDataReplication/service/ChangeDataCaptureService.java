package com.ssc.cdc.CDCDataReplication.service;

import com.ssc.cdc.CDCDataReplication.model.Change;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.LogRecord;

@Component
public class ChangeDataCaptureService {

    private final CDCClient cdcClient;

    Map<String, List<Change>> intermediateDataStructure;


    @Autowired
    public ChangeDataCaptureService(CDCClient cdcClient) {
        this.cdcClient = cdcClient;
    }

    @Scheduled(fixedDelay = 5000) // Adjust the delay according to your needs
    public void captureChanges() {
        List<LogRecord> logRecords = cdcClient.readTransactionLogs();

        List<Change> changes = processLogRecords(logRecords);

        storeChangesInIntermediateDataStructure(changes);
    }

    private List<Change> processLogRecords(List<LogRecord> logRecords) {
        List<Change> changes = new ArrayList<>();

        for (LogRecord logRecord : logRecords) {
            Change change = extractChangeFromLogRecord(logRecord);
            if (change != null) {
                changes.add(change);
            }
        }

        return changes;
    }

    private Change extractChangeFromLogRecord(LogRecord logRecord) {
        // Extract changes (table, columns, operation) from a log record
        String tableName = logRecord.getTableName();
        String operation = logRecord.getOperation();
        Map<String, Object> columnValues = logRecord.getColumnValues();

        // Determine if the log record is for a relevant table (E02, E45, or E55)
        if (!isRelevantTable(tableName)) {
            return null; // Skip log records for irrelevant tables
        }

        // Create a new Change object and populate the details
        Change change = new Change();
        change.setTableName(tableName);
        change.setOperation(operation);
        change.setColumnValues(columnValues);

        return change;
    }

    private boolean isRelevantTable(String tableName) {
        // Determine if the given table name is relevant (E02, E45, or E55)
        // You can implement your own logic based on the table names
        return tableName.equals("E02") || tableName.equals("E45") || tableName.equals("E55");
    }

    private void storeChangesInIntermediateDataStructure(LogRecord logRecord) {
        // Store the extracted changes in an intermediate data structure (e.g., a List or Map)
        String tableName = logRecord.
        List<LogRecord> tableChanges = intermediateDataStructure.getOrDefault(tableName, new ArrayList<>());
        tableChanges.add(logRecord);
        intermediateDataStructure.put(tableName, tableChanges);
    }
}
