package com.ssc.cdc.CDCDataReplication.model;

import jakarta.annotation.Nullable;
import jakarta.persistence.Entity;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@Nullable
public class E02 {
    private int id;
    private int price;
}
