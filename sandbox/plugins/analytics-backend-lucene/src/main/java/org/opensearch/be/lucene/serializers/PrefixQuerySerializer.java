/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * Serializer for the PREFIX_QUERY relevance function — passes the value verbatim to Lucene.
 */
public class PrefixQuerySerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "prefix_query";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new PrefixQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        PrefixQueryBuilder prefixQb = (PrefixQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "case_insensitive" -> prefixQb.caseInsensitive(Boolean.parseBoolean(entry.getValue()));
                case "rewrite" -> prefixQb.rewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
