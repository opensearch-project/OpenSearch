/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.ContextAwareGroupingScript;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;

/**
 * Field type for namespace field mapper
 *
 * @opensearch.internal
 */
public class ContextAwareGroupingFieldType extends CompositeMappedFieldType {

    private ContextAwareGroupingScript compiledScript;

    public ContextAwareGroupingFieldType(final List<String> fields, final ContextAwareGroupingScript compiledScript) {
        super(
            ContextAwareGroupingFieldMapper.CONTENT_TYPE,
            false,
            false,
            false,
            TextSearchInfo.NONE,
            Collections.emptyMap(),
            fields,
            CompositeFieldType.NAMESPACE
        );
        this.compiledScript = compiledScript;
    }

    public ContextAwareGroupingScript compiledScript() {
        return compiledScript;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        throw new UnsupportedOperationException("valueFetcher is not supported for namespace field");
    }

    @Override
    public String typeName() {
        return ContextAwareGroupingFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        // TODO: rethink this
        throw new UnsupportedOperationException("Term query is not supported for namespace field");
    }
}
