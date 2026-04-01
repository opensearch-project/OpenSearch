/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link FieldStorageInfo} for index fields from {@link IndexMetadata}.
 *
 * <p>At the coordinator, we only have {@link MappingMetadata} (raw source map).
 * {@code MappedFieldType} with {@code hasDocValues()} / {@code isSearchable()} is
 * only available at the data node via {@code MapperService}.
 *
 * <p>TODO: introduce a coordinator-level typed field metadata API so we don't
 * parse raw maps here. Or defer field storage resolution to the data node planner
 * where {@code MappedFieldType} is available.
 *
 * @opensearch.internal
 */
public class FieldStorageResolver {

    private FieldStorageResolver() {}

    @SuppressWarnings("unchecked")
    public static List<FieldStorageInfo> resolve(IndexMetadata indexMetadata, List<String> fieldNames) {
        String indexName = indexMetadata.getIndex().getName();
        String primaryFormat = indexMetadata.getSettings().get("index.composite.primary_data_format", "lucene");

        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            throw new IllegalStateException("No mapping found for index [" + indexName + "]");
        }

        Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
        if (properties == null) {
            throw new IllegalStateException("No properties in mapping for index [" + indexName + "]");
        }

        List<FieldStorageInfo> result = new ArrayList<>();
        for (String fieldName : fieldNames) {
            Map<String, Object> fieldProps = (Map<String, Object>) properties.get(fieldName);
            if (fieldProps == null) {
                throw new IllegalStateException("Field [" + fieldName + "] not found in mapping for index [" + indexName + "]");
            }
            result.add(resolveField(fieldName, fieldProps, primaryFormat));
        }
        return result;
    }

    private static FieldStorageInfo resolveField(String fieldName, Map<String, Object> fieldProps, String primaryFormat) {
        String fieldType = (String) fieldProps.get("type");
        if (fieldType == null) {
            throw new IllegalStateException("Field [" + fieldName + "] has no type in mapping");
        }

        // TODO: use MappedFieldType.hasDocValues() / isSearchable() when available
        boolean hasDocValues = Boolean.TRUE.equals(fieldProps.get("doc_values"))
            || (fieldProps.get("doc_values") == null && !"text".equals(fieldType));
        boolean isIndexed = Boolean.TRUE.equals(fieldProps.get("index"))
            || fieldProps.get("index") == null;

        if (!hasDocValues && !isIndexed) {
            throw new IllegalStateException("Field [" + fieldName + "] has neither doc_values nor index");
        }

        // TODO: data format per field should come from MappingMetadata directly
        List<String> docValueFormats = hasDocValues ? List.of(primaryFormat) : List.of();
        List<String> indexFormats = isIndexed ? List.of("lucene") : List.of();

        return new FieldStorageInfo(fieldName, fieldType, docValueFormats, indexFormats, false);
    }
}
