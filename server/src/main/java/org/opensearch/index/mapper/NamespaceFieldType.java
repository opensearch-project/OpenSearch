package org.opensearch.index.mapper;

import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.NamespaceScript;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;

public class NamespaceFieldType extends CompositeMappedFieldType {

    private NamespaceScript compiledScript;

    public NamespaceFieldType(final List<String> fields, final NamespaceScript compiledScript) {
        super(NamespaceFieldMapper.CONTENT_TYPE,false, false, false, TextSearchInfo.NONE,
            Collections.emptyMap(), fields, CompositeFieldType.NAMESPACE);
        this.compiledScript = compiledScript;
    }

    public NamespaceScript compiledScript() {
        return compiledScript;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        throw new UnsupportedOperationException("valueFetcher is not supported for namespace field");
    }

    @Override
    public String typeName() {
        return NamespaceFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        //TODO: rethink this
        throw new UnsupportedOperationException("Term query is not supported for namespace field");
    }
}
