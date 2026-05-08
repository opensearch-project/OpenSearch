/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves per-field storage metadata from {@link IndexMetadata}.
 *
 * <p>Uses the index's {@code index.composite.primary_data_format} setting as the
 * doc value format for all fields that have doc values. Index formats are always
 * {@code "lucene"} for fields explicitly marked {@code index: true}. Stored fields
 * are {@code "lucene"} for fields explicitly marked {@code store: true}.
 *
 * <p>TODO: Replace with actual per-field format metadata once the indexing team adds
 * {@code doc_value_formats} / {@code index_formats} to MappingMetadata.
 *
 * @opensearch.internal
 */
public class FieldStorageResolver {

    // TODO: import from CompositeEnginePlugin.PRIMARY_DATA_FORMAT once composite-common
    // exposes it as a shared constant accessible to analytics-engine.
    static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";

    private static final String LUCENE_FORMAT = "lucene";

    private final Map<String, FieldStorageInfo> fieldStorage;

    /**
     * Test constructor — explicit per-field storage, bypasses IndexMetadata inference.
     * Allows tests to declare hybrid fields (e.g. doc values in both parquet and lucene)
     * without needing actual IndexMetadata.
     *
     * TODO: remove once FieldStorageResolver is integrated with actual per-field format
     * metadata from MappingMetadata — tests should use real mappings at that point.
     */
    FieldStorageResolver(Map<String, FieldStorageInfo> fieldStorage) {
        this.fieldStorage = new HashMap<>(fieldStorage);
    }

    @SuppressWarnings("unchecked")
    public FieldStorageResolver(IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String primaryFormat = indexMetadata.getSettings().get(PRIMARY_DATA_FORMAT_SETTING, LUCENE_FORMAT);

        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            throw new IllegalStateException("No mapping found for index [" + indexName + "]");
        }
        Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
        if (properties == null) {
            throw new IllegalStateException("No properties in mapping for index [" + indexName + "]");
        }

        this.fieldStorage = new HashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null) {
                throw new IllegalStateException("Field [" + fieldName + "] has no type in mapping");
            }
            this.fieldStorage.put(fieldName, resolveField(fieldName, fieldType, fieldProps, primaryFormat));
        }
    }

    /** Resolves storage info for the requested fields in order. */
    public List<FieldStorageInfo> resolve(List<String> fieldNames) {
        List<FieldStorageInfo> result = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            FieldStorageInfo info = fieldStorage.get(fieldName);
            if (info == null) {
                throw new IllegalStateException("Field [" + fieldName + "] not found in field storage for index");
            }
            result.add(info);
        }
        return result;
    }

    private static FieldStorageInfo resolveField(String fieldName, String fieldType, Map<String, Object> fieldProps, String primaryFormat) {
        // Doc values: present for all types unless explicitly disabled
        boolean hasDocValues = !Boolean.FALSE.equals(fieldProps.get("doc_values"));

        // Index: only when explicitly set to false in mapping - enabled by default.
        boolean isIndexed = !Boolean.FALSE.equals(fieldProps.get("index"));

        // Stored fields: only when explicitly set to true in mapping
        boolean isStored = Boolean.TRUE.equals(fieldProps.get("store"));

        List<String> docValueFormats = hasDocValues ? List.of(primaryFormat) : List.of();
        List<String> indexFormats = isIndexed ? List.of(LUCENE_FORMAT) : List.of();
        List<String> storedFieldFormats = isStored ? List.of(LUCENE_FORMAT) : List.of();

        if (docValueFormats.isEmpty() && indexFormats.isEmpty() && storedFieldFormats.isEmpty()) {
            throw new IllegalStateException("Field [" + fieldName + "] has no storage in any format");
        }

        return new FieldStorageInfo(
            fieldName,
            fieldType,
            FieldType.fromMappingType(fieldType),
            docValueFormats,
            indexFormats,
            storedFieldFormats,
            false
        );
    }
}
