/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves per-field storage metadata by consulting each backend's {@link DataFormat}
 * capabilities. For each field, determines which formats provide doc values
 * ({@link Capability#COLUMNAR_STORAGE}), indices ({@link Capability#FULL_TEXT_SEARCH},
 * {@link Capability#POINT_RANGE}), and stored fields ({@link Capability#STORED_FIELDS}).
 *
 * <p>Production constructor queries backends' {@link DataFormat#supportedFields()} to
 * build per-field storage info. Test constructor accepts explicit per-field storage.
 *
 * @opensearch.internal
 */
public class FieldStorageResolver {

    private final Map<String, FieldStorageInfo> fieldStorage;
    private final List<String> docValueFormats;

    /**
     * Production: resolves per-field storage from IndexMetadata and backend capabilities.
     *
     * <p>LIMITATION: This infers storage from what each DataFormat DECLARES it can support
     * (via FieldTypeCapabilities), not from what the index ACTUALLY stores. A format declaring
     * COLUMNAR_STORAGE for "integer" doesn't mean this index has integer doc values in that format.
     *
     * <p>The indexing side has no per-field format metadata at the coordinator level today:
     * - DataFormat.supportedFields() = capability, not actual storage
     * - Segment.dfGroupedSearchableFiles = per-segment format info, data node only
     * - DataFormatRegistry has TODOs to filter by index settings/mapper service
     * - primary_data_format index setting is the only coordinator-level hint (index-level, not field-level)
     *
     * <p>TODO: Replace inference with actual per-field format metadata once the indexing team adds
     * it to MappingMetadata or IndexMetadata. Until then, this over-estimates viable backends —
     * the shard-level cost function must handle the mismatch. Consider using primary_data_format
     * as a narrowing hint in the interim.
     */
    @SuppressWarnings("unchecked")
    public FieldStorageResolver(IndexMetadata indexMetadata, List<SearchBackEndPlugin<?>> backends) {
        String indexName = indexMetadata.getIndex().getName();

        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            throw new IllegalStateException("No mapping found for index [" + indexName + "]");
        }

        Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
        if (properties == null) {
            throw new IllegalStateException("No properties in mapping for index [" + indexName + "]");
        }

        // Build format → capabilities lookup from all backends
        Map<String, Map<String, FieldTypeCapabilities>> formatCapabilities = buildFormatCapabilities(backends);

        this.fieldStorage = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null) {
                throw new IllegalStateException("Field [" + fieldName + "] has no type in mapping");
            }
            this.fieldStorage.put(fieldName, resolveField(fieldName, fieldType, fieldProps, formatCapabilities));
        }
        this.docValueFormats = computeDocValueFormats(this.fieldStorage);
    }

    /**
     * Test/future: explicit per-field storage info.
     * Simulates hybrid indices where doc values exist in multiple formats.
     */
    public FieldStorageResolver(Map<String, FieldStorageInfo> fieldStorage) {
        this.fieldStorage = fieldStorage;
        this.docValueFormats = computeDocValueFormats(fieldStorage);
    }

    /** Resolves storage info for the requested fields. */
    public List<FieldStorageInfo> resolve(List<String> fieldNames) {
        List<FieldStorageInfo> result = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            FieldStorageInfo info = fieldStorage.get(fieldName);
            if (info == null) {
                throw new IllegalStateException("Field [" + fieldName + "] not found in field storage");
            }
            result.add(info);
        }
        return result;
    }

    /** Returns all unique data formats that hold doc values across all fields. Precomputed at creation. */
    public List<String> docValueFormats() {
        return docValueFormats;
    }

    /** Returns true if the field has doc values in any format. */
    public boolean hasDocValues(String fieldName) {
        FieldStorageInfo info = fieldStorage.get(fieldName);
        return info != null && !info.getDocValueFormats().isEmpty();
    }

    /** Returns true if the field has an inverted index or point range in any format. */
    public boolean isIndexed(String fieldName) {
        FieldStorageInfo info = fieldStorage.get(fieldName);
        return info != null && !info.getIndexFormats().isEmpty();
    }

    /** Returns the first format that provides doc values for this field, or null. */
    public String getDocValueFormat(String fieldName) {
        FieldStorageInfo info = fieldStorage.get(fieldName);
        if (info == null || info.getDocValueFormats().isEmpty()) return null;
        return info.getDocValueFormats().get(0);
    }

    /** Returns storage info for a field, or null if not found. */
    public FieldStorageInfo getFieldInfo(String fieldName) {
        return fieldStorage.get(fieldName);
    }

    private static List<String> computeDocValueFormats(Map<String, FieldStorageInfo> fieldStorage) {
        List<String> formats = new ArrayList<>();
        for (FieldStorageInfo info : fieldStorage.values()) {
            for (String format : info.getDocValueFormats()) {
                if (!formats.contains(format)) {
                    formats.add(format);
                }
            }
        }
        return formats;
    }

    /**
     * Builds a lookup: formatName → fieldType → FieldTypeCapabilities
     * from all backends' DataFormats.
     */
    private static Map<String, Map<String, FieldTypeCapabilities>> buildFormatCapabilities(
            List<SearchBackEndPlugin<?>> backends) {
        Map<String, Map<String, FieldTypeCapabilities>> result = new LinkedHashMap<>();
        for (SearchBackEndPlugin<?> backend : backends) {
            for (DataFormat format : backend.getSupportedFormats()) {
                Map<String, FieldTypeCapabilities> byFieldType = result.computeIfAbsent(
                    format.name(), k -> new LinkedHashMap<>());
                for (FieldTypeCapabilities cap : format.supportedFields()) {
                    byFieldType.put(cap.fieldType(), cap);
                }
            }
        }
        return result;
    }

    private static FieldStorageInfo resolveField(String fieldName, String fieldType,
                                                  Map<String, Object> fieldProps,
                                                  Map<String, Map<String, FieldTypeCapabilities>> formatCapabilities) {
        List<String> docValueFormats = new ArrayList<>();
        List<String> indexFormats = new ArrayList<>();
        List<String> storedFieldFormats = new ArrayList<>();

        for (Map.Entry<String, Map<String, FieldTypeCapabilities>> formatEntry : formatCapabilities.entrySet()) {
            String formatName = formatEntry.getKey();
            FieldTypeCapabilities caps = formatEntry.getValue().get(fieldType);
            if (caps == null) {
                continue;
            }
            if (caps.capabilities().contains(Capability.COLUMNAR_STORAGE)) {
                docValueFormats.add(formatName);
            }
            if (caps.capabilities().contains(Capability.FULL_TEXT_SEARCH)
                    || caps.capabilities().contains(Capability.POINT_RANGE)) {
                indexFormats.add(formatName);
            }
            if (caps.capabilities().contains(Capability.STORED_FIELDS)) {
                storedFieldFormats.add(formatName);
            }
        }

        // Respect mapping overrides: doc_values=false or index=false
        boolean hasDocValues = Boolean.TRUE.equals(fieldProps.get("doc_values"))
            || (fieldProps.get("doc_values") == null && !"text".equals(fieldType));
        boolean isIndexed = Boolean.TRUE.equals(fieldProps.get("index"))
            || fieldProps.get("index") == null;

        if (!hasDocValues) {
            docValueFormats = List.of();
        }
        if (!isIndexed) {
            indexFormats = List.of();
        }

        if (docValueFormats.isEmpty() && indexFormats.isEmpty() && storedFieldFormats.isEmpty()) {
            throw new IllegalStateException("Field [" + fieldName + "] has no storage in any format");
        }

        return new FieldStorageInfo(fieldName, fieldType, FieldType.fromMappingType(fieldType),
            docValueFormats, indexFormats, storedFieldFormats, false);
    }
}
