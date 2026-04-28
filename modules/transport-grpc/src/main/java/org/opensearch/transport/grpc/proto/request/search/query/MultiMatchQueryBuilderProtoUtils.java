/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.transport.grpc.proto.request.search.OperatorProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting MultiMatchQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of bool queries
 * into their corresponding OpenSearch MultiMatchQueryBuilder implementations for search operations.
 */
class MultiMatchQueryBuilderProtoUtils {

    private MultiMatchQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MultiMatchQuery to an OpenSearch MultiMatchQueryBuilder.
     * Similar to {@link MultiMatchQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MultiMatchQueryBuilder with the appropriate fields, type, analyzer, slop, fuzziness, etc.
     *
     * @param multiMatchQueryProto The Protocol Buffer MultiMatchQuery object
     * @return A configured MultiMatchQueryBuilder instance
     * @throws IllegalArgumentException if the query is null or missing required fields
     */
    static MultiMatchQueryBuilder fromProto(MultiMatchQuery multiMatchQueryProto) {
        Object value = multiMatchQueryProto.getQuery();
        Map<String, Float> fieldsBoosts = new HashMap<>();
        MultiMatchQueryBuilder.Type type = MultiMatchQueryBuilder.DEFAULT_TYPE;
        String analyzer = null;
        int slop = MultiMatchQueryBuilder.DEFAULT_PHRASE_SLOP;
        Fuzziness fuzziness = null;
        int prefixLength = MultiMatchQueryBuilder.DEFAULT_PREFIX_LENGTH;
        int maxExpansions = MultiMatchQueryBuilder.DEFAULT_MAX_EXPANSIONS;
        Operator operator = MultiMatchQueryBuilder.DEFAULT_OPERATOR;
        String minimumShouldMatch = null;
        String fuzzyRewrite = null;
        Float tieBreaker = null;
        Boolean lenient = null;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = MultiMatchQueryBuilder.DEFAULT_ZERO_TERMS_QUERY;
        boolean autoGenerateSynonymsPhraseQuery = true;
        boolean fuzzyTranspositions = MultiMatchQueryBuilder.DEFAULT_FUZZY_TRANSPOSITIONS;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        if (multiMatchQueryProto.getFieldsCount() > 0) {
            for (String field : multiMatchQueryProto.getFieldsList()) {
                fieldsBoosts.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
            }
        }

        if (multiMatchQueryProto.hasType()) {
            switch (multiMatchQueryProto.getType()) {
                case TEXT_QUERY_TYPE_BEST_FIELDS:
                    type = MultiMatchQueryBuilder.Type.BEST_FIELDS;
                    break;
                case TEXT_QUERY_TYPE_MOST_FIELDS:
                    type = MultiMatchQueryBuilder.Type.MOST_FIELDS;
                    break;
                case TEXT_QUERY_TYPE_CROSS_FIELDS:
                    type = MultiMatchQueryBuilder.Type.CROSS_FIELDS;
                    break;
                case TEXT_QUERY_TYPE_PHRASE:
                    type = MultiMatchQueryBuilder.Type.PHRASE;
                    break;
                case TEXT_QUERY_TYPE_PHRASE_PREFIX:
                    type = MultiMatchQueryBuilder.Type.PHRASE_PREFIX;
                    break;
                case TEXT_QUERY_TYPE_BOOL_PREFIX:
                    type = MultiMatchQueryBuilder.Type.BOOL_PREFIX;
                    break;
                default:
                    // Keep default
            }
        }

        if (multiMatchQueryProto.hasAnalyzer()) {
            analyzer = multiMatchQueryProto.getAnalyzer();
        }

        if (multiMatchQueryProto.hasBoost()) {
            boost = multiMatchQueryProto.getBoost();
        }

        if (multiMatchQueryProto.hasSlop()) {
            slop = multiMatchQueryProto.getSlop();
        }

        if (multiMatchQueryProto.hasFuzziness()) {
            org.opensearch.protobufs.Fuzziness fuzzinessProto = multiMatchQueryProto.getFuzziness();
            if (fuzzinessProto.hasString()) {
                fuzziness = Fuzziness.build(fuzzinessProto.getString());
            } else if (fuzzinessProto.hasInt32()) {
                fuzziness = Fuzziness.fromEdits(fuzzinessProto.getInt32());
            }
        }

        if (multiMatchQueryProto.hasPrefixLength()) {
            prefixLength = multiMatchQueryProto.getPrefixLength();
        }

        if (multiMatchQueryProto.hasMaxExpansions()) {
            maxExpansions = multiMatchQueryProto.getMaxExpansions();
        }

        if (multiMatchQueryProto.hasOperator()
            && multiMatchQueryProto.getOperator() != org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED) {
            operator = OperatorProtoUtils.fromEnum(multiMatchQueryProto.getOperator());
        }

        if (multiMatchQueryProto.hasMinimumShouldMatch()) {
            if (multiMatchQueryProto.getMinimumShouldMatch().hasString()) {
                minimumShouldMatch = multiMatchQueryProto.getMinimumShouldMatch().getString();
            } else if (multiMatchQueryProto.getMinimumShouldMatch().hasInt32()) {
                minimumShouldMatch = String.valueOf(multiMatchQueryProto.getMinimumShouldMatch().getInt32());
            }
        }

        if (multiMatchQueryProto.hasFuzzyRewrite()) {
            fuzzyRewrite = multiMatchQueryProto.getFuzzyRewrite();
        }

        if (multiMatchQueryProto.hasTieBreaker()) {
            tieBreaker = multiMatchQueryProto.getTieBreaker();
        }

        if (multiMatchQueryProto.hasLenient()) {
            lenient = multiMatchQueryProto.getLenient();
        }

        if (multiMatchQueryProto.hasZeroTermsQuery()) {
            switch (multiMatchQueryProto.getZeroTermsQuery()) {
                case ZERO_TERMS_QUERY_NONE:
                    zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
                    break;
                case ZERO_TERMS_QUERY_ALL:
                    zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
                    break;
                case ZERO_TERMS_QUERY_UNSPECIFIED:
                    // Keep default
                    break;
                default:
                    // Keep default
            }
        }

        if (multiMatchQueryProto.hasXName()) {
            queryName = multiMatchQueryProto.getXName();
        }

        if (multiMatchQueryProto.hasAutoGenerateSynonymsPhraseQuery()) {
            autoGenerateSynonymsPhraseQuery = multiMatchQueryProto.getAutoGenerateSynonymsPhraseQuery();
        }

        if (multiMatchQueryProto.hasFuzzyTranspositions()) {
            fuzzyTranspositions = multiMatchQueryProto.getFuzzyTranspositions();
        }

        if (slop != MultiMatchQueryBuilder.DEFAULT_PHRASE_SLOP && type == MultiMatchQueryBuilder.Type.BOOL_PREFIX) {
            throw new IllegalArgumentException("slop not allowed for type [" + type + "]");
        }

        // Create the builder with all the extracted values - matching fromXContent exactly
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder(value).fields(fieldsBoosts)
            .type(type)
            .analyzer(analyzer)
            .fuzziness(fuzziness)
            .fuzzyRewrite(fuzzyRewrite)
            .maxExpansions(maxExpansions)
            .minimumShouldMatch(minimumShouldMatch)
            .operator(operator)
            .prefixLength(prefixLength)
            .slop(slop)
            .tieBreaker(tieBreaker)
            .zeroTermsQuery(zeroTermsQuery)
            .autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery)
            .boost(boost)
            .queryName(queryName)
            .fuzzyTranspositions(fuzzyTranspositions);

        if (lenient != null) {
            builder.lenient(lenient);
        }

        return builder;
    }
}
