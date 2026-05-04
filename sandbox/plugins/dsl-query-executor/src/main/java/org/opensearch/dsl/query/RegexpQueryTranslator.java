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
import org.opensearch.index.query.RegexpQueryBuilder;

/**
 * Converts {@link RegexpQueryBuilder} to {@link UnresolvedQueryCall}.
 * Translation to SQL SIMILAR TO is lossy — Lucene regex operates on terms in inverted index,
 * SQL regex operates on full field values. UnresolvedQueryCall preserves Lucene semantics.
 */
public class RegexpQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return RegexpQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        return new UnresolvedQueryCall(ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BOOLEAN), query);
    }
}
