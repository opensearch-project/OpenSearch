/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

/**
 * Lightweight MappedFieldType created by {@link FieldAssignmentResolver} to carry
 * per-format capability flags (isIndexed, isStored, hasDocValues) for a field.
 * Not used for query execution — only for the indexing write path.
 */
@ExperimentalApi
public final class AssignedFieldType extends MappedFieldType {

    private final String type;

    public AssignedFieldType(String name, String typeName, boolean isIndexed, boolean isStored, boolean hasDocValues) {
        super(name, isIndexed, isStored, hasDocValues, TextSearchInfo.NONE, null);
        this.type = typeName;
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return null;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new UnsupportedOperationException("AssignedFieldType does not support queries");
    }
}
