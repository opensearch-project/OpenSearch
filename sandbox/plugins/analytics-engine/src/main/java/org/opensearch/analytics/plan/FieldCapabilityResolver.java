/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.indices.IndicesService;

/**
 * Resolves field-level capabilities from OpenSearch index mappings.
 * Used by the validator (Phase 1) to check whether a field supports
 * full-text search (i.e., has a Lucene inverted index).
 *
 * Field capabilities come from IndexService.mapperService().fieldType(fieldName),
 * NOT from the Calcite schema — the Calcite schema only carries type information,
 * not index structure.
 */
public final class FieldCapabilityResolver {

    private final IndicesService indicesService;
    private final ClusterService clusterService;

    public FieldCapabilityResolver(IndicesService indicesService, ClusterService clusterService) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    /**
     * Returns true if the given field in the given index has a Lucene inverted index
     * (i.e., MappedFieldType.isSearchable() == true).
     *
     * @param indexName the index/table name
     * @param fieldName the field name
     * @return true if the field is indexed in Lucene
     */
    public boolean isFieldIndexed(String indexName, String fieldName) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) return false;
        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) return false;
        MappedFieldType fieldType = indexService.mapperService().fieldType(fieldName);
        return fieldType != null && fieldType.isSearchable();
    }

    /**
     * Returns true if the given field supports full-text queries
     * (i.e., has TextSearchInfo with a non-NONE analyzer).
     *
     * @param indexName the index/table name
     * @param fieldName the field name
     * @return true if the field supports full-text search
     */
    public boolean isFieldFullTextSearchable(String indexName, String fieldName) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) return false;
        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) return false;
        MappedFieldType fieldType = indexService.mapperService().fieldType(fieldName);
        if (fieldType == null) return false;
        TextSearchInfo tsi = fieldType.getTextSearchInfo();
        return tsi != null && tsi != TextSearchInfo.NONE;
    }
}
