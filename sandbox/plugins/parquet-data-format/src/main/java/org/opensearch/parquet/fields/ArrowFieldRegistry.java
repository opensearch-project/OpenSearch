/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.parquet.fields.plugins.CoreDataFieldPlugin;
import org.opensearch.parquet.fields.plugins.MetadataFieldPlugin;
import org.opensearch.parquet.fields.plugins.ParquetFieldPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    private static void initialize() {
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
                    "Field type [" + fieldType + "] is already registered. Plugin [" + pluginName + "] cannot override it."
                );
            }
            FIELD_REGISTRY.put(fieldType, parquetField);
        }
    }

    /**
     * Returns the ParquetField for the given field type.
     * @param fieldType the field type name
     * @return the registered ParquetField, or null if not found
     */
    public static ParquetField getParquetField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    /**
     * Returns an unmodifiable view of all registered fields.
     * @return map of field type names to ParquetField implementations
     */
    public static Map<String, ParquetField> getRegisteredFields() {
        return Collections.unmodifiableMap(FIELD_REGISTRY);
    }

    /**
     * Returns the supported field type capabilities for all registered fields,
     * querying each {@link ParquetField} for its supported capabilities.
     *
     * @return unmodifiable set of {@link FieldTypeCapabilities} for all registered field types
     */
    public static Set<FieldTypeCapabilities> getSupportedFieldCapabilities() {
        return FIELD_REGISTRY.entrySet()
            .stream()
            .map(e -> new FieldTypeCapabilities(e.getKey(), e.getValue().supportedCapabilities()))
            .collect(Collectors.toUnmodifiableSet());
    }
}
