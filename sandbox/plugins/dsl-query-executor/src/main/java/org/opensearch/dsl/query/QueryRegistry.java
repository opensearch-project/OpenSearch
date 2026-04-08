/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry of {@link QueryTranslator}s keyed by QueryBuilder class.
 * O(1) lookup — known query types get standard Rex, unknown ones get {@link UnresolvedQueryCall}.
 */
public class QueryRegistry {

    private final Map<Class<? extends QueryBuilder>, QueryTranslator> translators = new HashMap<>();

    /** Creates an empty registry. */
    public QueryRegistry() {}

    /**
     * Registers a translator.
     *
     * @param translator the translator to register
     */
    public void register(QueryTranslator translator) {
        translators.put(translator.getQueryType(), translator);
    }

    /**
     * Converts a query using the registered translator for its type.
     * If no translator is registered, wraps the query in an {@link UnresolvedQueryCall}
     * for the analytics engine's optimizer to resolve or reject.
     *
     * @param query the query builder to convert
     * @param ctx the conversion context
     * @return the resulting RexNode — standard Rex or UnresolvedQueryCall
     * @throws ConversionException if a registered translator fails
     */
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        QueryTranslator translator = translators.get(query.getClass());
        if (translator != null) {
            return translator.convert(query, ctx);
        }
        // No translator — wrap as unresolved for downstream to handle
        return new UnresolvedQueryCall(
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
            query
        );
    }
}
