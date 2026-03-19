/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;
import org.opensearch.parquet.fields.plugins.CoreDataFieldPlugin;
import org.opensearch.parquet.fields.plugins.MetadataFieldPlugin;
import org.opensearch.parquet.fields.plugins.ParquetFieldPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry mapping OpenSearch field types to their corresponding Parquet field implementations.
 * Populated via {@link ParquetFieldPlugin} registrations.
 */
public final class ArrowFieldRegistry {

    private static final Map<String, ParquetField> FIELD_REGISTRY = new ConcurrentHashMap<>();

    static {
        initialize();
    }

    private ArrowFieldRegistry() {}

    public static synchronized void initialize() {
        registerPlugin(new CoreDataFieldPlugin(), "CoreDataFields");
        registerPlugin(new MetadataFieldPlugin(), "MetadataFields");
    }

    private static void registerPlugin(ParquetFieldPlugin plugin, String pluginName) {
        Map<String, ParquetField> fields = plugin.getParquetFields();
        if (fields == null || fields.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ParquetField> entry : fields.entrySet()) {
            String fieldType = entry.getKey();
            ParquetField parquetField = entry.getValue();
            if (fieldType == null || fieldType.trim().isEmpty()) {
                throw new IllegalArgumentException("Field type name cannot be null or empty");
            }
            if (parquetField == null) {
                throw new IllegalArgumentException("ParquetField implementation cannot be null for type [" + fieldType + "]");
            }
            if (FIELD_REGISTRY.containsKey(fieldType)) {
                throw new IllegalArgumentException(
                    String.format("Field type [%s] is already registered. Plugin [%s] cannot override it.", fieldType, pluginName)
                );
            }
            FIELD_REGISTRY.put(fieldType, parquetField);
        }
        FIELD_REGISTRY.put(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField());
    }

    public static ParquetField getParquetField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    public static Map<String, ParquetField> getRegisteredFields() {
        return Collections.unmodifiableMap(FIELD_REGISTRY);
    }
}
