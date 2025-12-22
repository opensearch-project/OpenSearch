/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import com.parquet.parquetdataformat.plugins.fields.CoreDataFieldPlugin;
import com.parquet.parquetdataformat.plugins.fields.MetadataFieldPlugin;
import com.parquet.parquetdataformat.plugins.fields.ParquetFieldPlugin;
import org.opensearch.index.mapper.SeqNoFieldMapper;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for mapping OpenSearch field types to their corresponding Parquet field implementations.
 * This class maintains a centralized mapping between OpenSearch field type names and their
 * Arrow/Parquet field representations, enabling efficient field type resolution during
 * schema creation and data processing.
 *
 * <p>The registry is initialized once during class loading and provides thread-safe
 * read-only access to field mappings.</p>
 */
public final class ArrowFieldRegistry {

    /**
     * All registered field mappings (thread-safe, mutable)
     */
    private static final Map<String, ParquetField> FIELD_REGISTRY = new ConcurrentHashMap<>();

    // Static initialization block to populate the field registry
    static {
        initialize();
    }

    // Private constructor to prevent instantiation of utility class
    private ArrowFieldRegistry() {
        throw new UnsupportedOperationException("Registry class should not be instantiated");
    }

    /**
     * Initialize the registry with all available plugins.
     * This method should be called during node startup after all plugins are loaded.
     */
    public static synchronized void initialize() {
        // Always register core plugins first
        registerCorePlugins();
    }

    /**
     * Register core OpenSearch field plugins.
     * These are always available and provide the foundation field type support.
     */
    private static void registerCorePlugins() {
        // Register core data fields
        registerPlugin(new CoreDataFieldPlugin(), "CoreDataFields");

        // REgister metadata fields
        registerPlugin(new MetadataFieldPlugin(), "MetadataFields");
    }
    /**
     * Register a single plugin's field types.
     */
    private static void registerPlugin(ParquetFieldPlugin plugin, String pluginName) {
        Map<String, ParquetField> fields = plugin.getParquetFields();

        if (fields != null && !fields.isEmpty()) {
            for (Map.Entry<String, ParquetField> entry : fields.entrySet()) {
                String fieldType = entry.getKey();
                ParquetField parquetField = entry.getValue();

                // Validate registration
                validateFieldRegistration(fieldType, parquetField, pluginName);

                // Check for conflicts
                if (FIELD_REGISTRY.containsKey(fieldType)) {
                    throw new IllegalArgumentException(
                        String.format("Field type [%s] is already registered. Plugin [%s] cannot override it.",
                            fieldType, pluginName)
                    );
                }

                FIELD_REGISTRY.put(fieldType, parquetField);
            }

            FIELD_REGISTRY.put(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField());
        }
    }

    private static void validateFieldRegistration(String fieldType, ParquetField parquetField, String source) {
        if (fieldType == null || fieldType.trim().isEmpty()) {
            throw new IllegalArgumentException("Field type name cannot be null or empty");
        }

        if (parquetField == null) {
            throw new IllegalArgumentException("ParquetField implementation cannot be null");
        }

        // Validate that the ParquetField can provide required Arrow types
        try {
            parquetField.getArrowType();
            parquetField.getFieldType();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid ParquetField implementation for type [%s] from source [%s]: %s",
                    fieldType, source, e.getMessage()), e
            );
        }
    }

    /**
     * Get registry statistics for monitoring and debugging.
     */
    public static RegistryStats getStats() {
        Set<String> allTypes = getRegisteredFieldNames();

        return new RegistryStats(
            FIELD_REGISTRY.size(),  // Single source of truth
            allTypes
        );
    }

    /**
     * Get all registered field type names.
     */
    public static Set<String> getRegisteredFieldNames() {
        return Collections.unmodifiableSet(FIELD_REGISTRY.keySet());
    }

    /**
     * Checks if a fieldtype is supported or not
     */
    public static boolean isFieldTypeSupported(String fieldType) { return FIELD_REGISTRY.containsKey(fieldType); }

    /**
     * Returns the ParquetField implementation for the specified OpenSearch field type, or null if not found.
     */
    public static ParquetField getParquetField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    public static class RegistryStats {
        private final int totalFields;
        private final Set<String> allFieldTypes;

        public RegistryStats(int totalFields, Set<String> allFieldTypes) {
            this.totalFields = totalFields;
            this.allFieldTypes = allFieldTypes;
        }

        // Getters
        public int getTotalFields() { return totalFields; }
        public Set<String> getAllFieldTypes() { return allFieldTypes; }

        @Override
        public String toString() {
            return String.format("RegistryStats{total=%d, }", totalFields);
        }
    }

}
