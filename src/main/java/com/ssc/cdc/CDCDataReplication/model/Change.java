package com.ssc.cdc.CDCDataReplication.model;

import lombok.Data;

import java.util.Map;


@Data
public class Change {
    private String tableName;
    private String operation;
    private Map<String, Object> columnValues;

    // Constructors, getters, and setters
}
