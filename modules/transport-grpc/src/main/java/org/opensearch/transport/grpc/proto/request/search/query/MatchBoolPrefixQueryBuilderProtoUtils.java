/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.protobufs.MatchBoolPrefixQuery;
import org.opensearch.transport.grpc.proto.request.search.OperatorProtoUtils;

/**
 * Utility class for converting MatchBoolPrefixQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of match_bool_prefix queries
 * into their corresponding OpenSearch MatchBoolPrefixQueryBuilder implementations for search operations.
 */
class MatchBoolPrefixQueryBuilderProtoUtils {

    private MatchBoolPrefixQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchBoolPrefixQuery to an OpenSearch MatchBoolPrefixQueryBuilder.
     * Similar to {@link MatchBoolPrefixQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchBoolPrefixQueryBuilder with the appropriate field name, value, operator, analyzer,
     * minimum_should_match, fuzziness, boost, and query name.
     *
     * @param matchBoolPrefixQueryProto The Protocol Buffer MatchBoolPrefixQuery object
     * @return A configured MatchBoolPrefixQueryBuilder instance
     * @throws IllegalArgumentException if the field name or value is null or empty
     */
    static MatchBoolPrefixQueryBuilder fromProto(MatchBoolPrefixQuery matchBoolPrefixQueryProto) {
        String fieldName = matchBoolPrefixQueryProto.getField();
        Object value = matchBoolPrefixQueryProto.getQuery();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        Operator operator = Operator.OR;
        String minimumShouldMatch = null;
        Fuzziness fuzziness = null;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansion = FuzzyQuery.defaultMaxExpansions;
        boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;
        String fuzzyRewrite = null;
        String queryName = null;

        if (matchBoolPrefixQueryProto.hasAnalyzer()) {
            analyzer = matchBoolPrefixQueryProto.getAnalyzer();
        }

        if (matchBoolPrefixQueryProto.hasOperator()
            && matchBoolPrefixQueryProto.getOperator() != org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED) {
            operator = OperatorProtoUtils.fromEnum(matchBoolPrefixQueryProto.getOperator());
        }

        if (matchBoolPrefixQueryProto.hasMinimumShouldMatch()) {
            org.opensearch.protobufs.MinimumShouldMatch minimumShouldMatchProto = matchBoolPrefixQueryProto.getMinimumShouldMatch();
            if (minimumShouldMatchProto.hasString()) {
                minimumShouldMatch = minimumShouldMatchProto.getString();
            } else if (minimumShouldMatchProto.hasInt32()) {
                minimumShouldMatch = String.valueOf(minimumShouldMatchProto.getInt32());
            }
        }

        if (matchBoolPrefixQueryProto.hasFuzziness()) {
            org.opensearch.protobufs.Fuzziness fuzzinessProto = matchBoolPrefixQueryProto.getFuzziness();
            if (fuzzinessProto.hasString()) {
                fuzziness = Fuzziness.build(fuzzinessProto.getString());
            } else if (fuzzinessProto.hasInt32()) {
                fuzziness = Fuzziness.fromEdits(fuzzinessProto.getInt32());
            }
        }

        if (matchBoolPrefixQueryProto.hasPrefixLength()) {
            prefixLength = matchBoolPrefixQueryProto.getPrefixLength();
        }

        if (matchBoolPrefixQueryProto.hasMaxExpansions()) {
            maxExpansion = matchBoolPrefixQueryProto.getMaxExpansions();
        }

        if (matchBoolPrefixQueryProto.hasFuzzyTranspositions()) {
            fuzzyTranspositions = matchBoolPrefixQueryProto.getFuzzyTranspositions();
        }

        if (matchBoolPrefixQueryProto.hasFuzzyRewrite()) {
            fuzzyRewrite = matchBoolPrefixQueryProto.getFuzzyRewrite();
        }

        if (matchBoolPrefixQueryProto.hasBoost()) {
            boost = matchBoolPrefixQueryProto.getBoost();
        }

        if (matchBoolPrefixQueryProto.hasXName()) {
            queryName = matchBoolPrefixQueryProto.getXName();
        }

        return new MatchBoolPrefixQueryBuilder(fieldName, value).analyzer(analyzer)
            .operator(operator)
            .minimumShouldMatch(minimumShouldMatch)
            .boost(boost)
            .queryName(queryName)
            .fuzziness(fuzziness)
            .prefixLength(prefixLength)
            .maxExpansions(maxExpansion)
            .fuzzyTranspositions(fuzzyTranspositions)
            .fuzzyRewrite(fuzzyRewrite);
    }
}
