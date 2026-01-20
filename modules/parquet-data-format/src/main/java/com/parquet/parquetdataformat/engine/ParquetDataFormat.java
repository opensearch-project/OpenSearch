package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.ParquetSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.DataFormat;

/**
 * Data format implementation for Parquet-based document storage.
 *
 * <p>This class integrates with OpenSearch's DataFormat interface to provide
 * Parquet file format support within the OpenSearch indexing pipeline. It
 * defines the configuration and behavior for the "parquet" data format.
 *
 * <p>The implementation provides hooks for:
 * <ul>
 *   <li>Data format specific settings configuration</li>
 *   <li>Cluster-level settings management</li>
 *   <li>Store configuration for Parquet-specific optimizations</li>
 *   <li>Format identification through the "parquet" name</li>
 * </ul>
 *
 * <p>This class serves as the entry point for registering Parquet format
 * capabilities with OpenSearch's execution engine framework, allowing
 * the system to recognize and utilize Parquet-based storage operations.
 */
public class ParquetDataFormat implements DataFormat {
    
    @Override
    public Setting<Settings> dataFormatSettings() {
        return ParquetSettings.PARQUET_SETTINGS;
    }

    @Override
    public Setting<Settings> clusterLeveldataFormatSettings() {
        return null;
    }

    @Override
    public String name() {
        return "parquet";
    }

    @Override
    public void configureStore() {

    }

    public static ParquetDataFormat PARQUET_DATA_FORMAT = new ParquetDataFormat();

    @Override
    public boolean equals(Object obj) {
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "ParquetDataFormat";
    }
}
