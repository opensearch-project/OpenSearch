/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;

/**
 * Translates a single OpenSearch query type to a Calcite RexNode.
 * One implementation per query type (term, match_all, range, bool, etc.).
 */
public interface QueryTranslator {

    /** Returns the concrete QueryBuilder class this translator handles. */
    Class<? extends QueryBuilder> getQueryType();

    /**
     * Converts the query to a Calcite RexNode filter expression.
     *
     * @param query the query builder to convert
     * @param ctx the conversion context
     * @return the resulting RexNode
     * @throws ConversionException if conversion fails
     */
    RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException;
}
