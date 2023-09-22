package com.ssc.cdc.CDCDataReplication.service;

import java.util.List;
import java.util.logging.LogRecord;

public interface ICDCClient {
    /**
     * Read the DB2 transaction logs and capture changes.
     * This method will continuously monitor the logs and return a list of log records with changes.
     *
     * @return List of log records containing changes.
     */
    List<LogRecord> readTransactionLogs();
}
