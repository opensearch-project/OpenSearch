package com.format.parquet.engine;

/**
 * Data format implementation for Parquet-based document storage.
 */
public class ParquetDataFormat implements org.opensearch.index.engine.exec.format.DataFormat {

    @Override
    public String name() {
        return "parquet";
    }

    public static ParquetDataFormat PARQUET_DATA_FORMAT = new ParquetDataFormat();

}
