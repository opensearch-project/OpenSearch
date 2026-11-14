/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core;

import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.be.lucene.fields.plugins.CoreDataFieldPlugin;
import org.opensearch.be.lucene.fields.plugins.LuceneFieldPlugin;
import org.opensearch.be.lucene.fields.plugins.MetadataFieldPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry mapping OpenSearch field types to their corresponding Lucene field implementations.
 * Populated via {@link LuceneFieldPlugin} registrations.
 */
public final class LuceneFieldRegistry {

    private static final Map<String, LuceneField> FIELD_REGISTRY = new ConcurrentHashMap<>();

    static {
        initialize();
    }

    private LuceneFieldRegistry() {}

    private static void initialize() {
        registerPlugin(new CoreDataFieldPlugin(), "CoreDataFields");
        registerPlugin(new MetadataFieldPlugin(), "MetadataFields");
    }

    private static void registerPlugin(LuceneFieldPlugin plugin, String pluginName) {
        Map<String, LuceneField> fields = plugin.getLuceneFields();
        if (fields == null || fields.isEmpty()) {
            return;
        }
        for (Map.Entry<String, LuceneField> entry : fields.entrySet()) {
            String fieldType = entry.getKey();
            LuceneField luceneField = entry.getValue();
            if (fieldType == null || fieldType.trim().isEmpty()) {
                throw new IllegalArgumentException("Field type name cannot be null or empty");
            }
            if (luceneField == null) {
                throw new IllegalArgumentException("LuceneField implementation cannot be null for type [" + fieldType + "]");
            }
            if (FIELD_REGISTRY.containsKey(fieldType)) {
                throw new IllegalArgumentException(
                    "Field type [" + fieldType + "] is already registered. Plugin [" + pluginName + "] cannot override it."
                );
            }
            FIELD_REGISTRY.put(fieldType, luceneField);
        }
    }

    /**
     * Returns the LuceneField for the given field type.
     * @param fieldType the field type name
     * @return the registered LuceneField, or null if not found
     */
    public static LuceneField getLuceneField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    /**
     * Returns an unmodifiable view of all registered fields.
     * @return map of field type names to LuceneField implementations
     */
    public static Map<String, LuceneField> getRegisteredFields() {
        return Collections.unmodifiableMap(FIELD_REGISTRY);
    }
}
