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
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MultiTermQueryRewrite;
import org.opensearch.protobufs.ZeroTermsQuery;
import org.opensearch.transport.grpc.proto.request.search.OperatorProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting MatchQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of match queries
 * into their corresponding OpenSearch MatchQueryBuilder implementations for search operations.
 */
class MatchQueryBuilderProtoUtils {

    private MatchQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchQuery to an OpenSearch MatchQueryBuilder.
     * Similar to {@link MatchQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchQueryBuilder with the appropriate field name, value, operator, analyzer, fuzziness,
     * prefix length, max expansions, fuzzy rewrite, fuzzy transpositions, lenient,
     * zero terms query, boost, and query name.
     *
     * @param matchQueryProto The Protocol Buffer MatchQuery object
     * @return A configured MatchQueryBuilder instance
     * @throws IllegalArgumentException if the field name or value is null or empty
     */
    static MatchQueryBuilder fromProto(org.opensearch.protobufs.MatchQuery matchQueryProto) {
        if (matchQueryProto == null) {
            throw new IllegalArgumentException("MatchQuery cannot be null");
        }

        String fieldName = matchQueryProto.getField();
        Object value = FieldValueProtoUtils.fromProto(matchQueryProto.getQuery(), false);
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String minimumShouldMatch = null;
        String analyzer = null;
        Operator operator = MatchQueryBuilder.DEFAULT_OPERATOR;
        Fuzziness fuzziness = null;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansions = FuzzyQuery.defaultMaxExpansions;
        boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;
        String fuzzyRewrite = null;
        boolean lenient = MatchQuery.DEFAULT_LENIENCY;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;
        boolean autoGenerateSynonymsPhraseQuery = true;
        String queryName = null;

        if (matchQueryProto.hasAnalyzer()) {
            analyzer = matchQueryProto.getAnalyzer();
        }

        if (matchQueryProto.hasBoost()) {
            boost = matchQueryProto.getBoost();
        }
        if (matchQueryProto.hasFuzziness()) {
            org.opensearch.protobufs.Fuzziness fuzzinessProto = matchQueryProto.getFuzziness();
            if (fuzzinessProto.hasString()) {
                fuzziness = Fuzziness.build(fuzzinessProto.getString());
            } else if (fuzzinessProto.hasInt32()) {
                fuzziness = Fuzziness.fromEdits(fuzzinessProto.getInt32());
            }
        }

        if (matchQueryProto.hasPrefixLength()) {
            prefixLength = matchQueryProto.getPrefixLength();
        }

        if (matchQueryProto.hasMaxExpansions()) {
            maxExpansions = matchQueryProto.getMaxExpansions();
        }

        if (matchQueryProto.hasOperator() && matchQueryProto.getOperator() != org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED) {
            operator = OperatorProtoUtils.fromEnum(matchQueryProto.getOperator());
        }

        if (matchQueryProto.hasMinimumShouldMatch()) {
            org.opensearch.protobufs.MinimumShouldMatch minimumShouldMatchProto = matchQueryProto.getMinimumShouldMatch();
            if (minimumShouldMatchProto.hasString()) {
                minimumShouldMatch = minimumShouldMatchProto.getString();
            } else if (minimumShouldMatchProto.hasInt32()) {
                minimumShouldMatch = String.valueOf(minimumShouldMatchProto.getInt32());
            }
        }

        if (matchQueryProto.hasFuzzyRewrite()) {
            MultiTermQueryRewrite rewriteEnum = matchQueryProto.getFuzzyRewrite();
            if (rewriteEnum != MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED) {
                fuzzyRewrite = ProtobufEnumUtils.convertToString(rewriteEnum);
            }
        }

        if (matchQueryProto.hasFuzzyTranspositions()) {
            fuzzyTranspositions = matchQueryProto.getFuzzyTranspositions();
        }

        if (matchQueryProto.hasLenient()) {
            lenient = matchQueryProto.getLenient();
        }

        if (matchQueryProto.hasZeroTermsQuery() && matchQueryProto.getZeroTermsQuery() != ZeroTermsQuery.ZERO_TERMS_QUERY_UNSPECIFIED) {
            if (matchQueryProto.getZeroTermsQuery() == org.opensearch.protobufs.ZeroTermsQuery.ZERO_TERMS_QUERY_ALL) {
                zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
            } else if (matchQueryProto.getZeroTermsQuery() == org.opensearch.protobufs.ZeroTermsQuery.ZERO_TERMS_QUERY_NONE) {
                zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
            }
        }

        if (matchQueryProto.hasXName()) {
            queryName = matchQueryProto.getXName();
        }

        if (matchQueryProto.hasAutoGenerateSynonymsPhraseQuery()) {
            autoGenerateSynonymsPhraseQuery = matchQueryProto.getAutoGenerateSynonymsPhraseQuery();
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder(fieldName, value);
        matchQuery.operator(operator);
        matchQuery.analyzer(analyzer);
        matchQuery.minimumShouldMatch(minimumShouldMatch);
        matchQuery.fuzziness(fuzziness);
        matchQuery.fuzzyRewrite(fuzzyRewrite);
        matchQuery.prefixLength(prefixLength);
        matchQuery.fuzzyTranspositions(fuzzyTranspositions);
        matchQuery.maxExpansions(maxExpansions);
        matchQuery.lenient(lenient);
        matchQuery.zeroTermsQuery(zeroTermsQuery);
        matchQuery.autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);
        return matchQuery;
    }
}
