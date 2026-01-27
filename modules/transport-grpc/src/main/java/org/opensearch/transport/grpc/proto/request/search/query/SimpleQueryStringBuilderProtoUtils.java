/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.protobufs.SimpleQueryStringQuery;
import org.opensearch.transport.grpc.proto.request.search.OperatorProtoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting SimpleQueryStringQuery Protocol Buffers to OpenSearch query objects.
 * This class provides methods to transform Protocol Buffer representations of simple query string queries
 * into their corresponding OpenSearch SimpleQueryStringBuilder implementations for search operations.
 */
class SimpleQueryStringBuilderProtoUtils {

    // Error message constants matching SimpleQueryStringBuilder
    static final String QUERY_TEXT_MISSING = "[" + SimpleQueryStringBuilder.NAME + "] query text missing";
    static final String UNKNOWN_FLAG_PREFIX = "Unknown " + SimpleQueryStringBuilder.NAME + " flag [";

    private SimpleQueryStringBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer SimpleQueryStringQuery to an OpenSearch SimpleQueryStringBuilder.
     * Similar to {@link SimpleQueryStringBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * SimpleQueryStringBuilder with all appropriate settings.
     *
     * @param simpleQueryStringProto The Protocol Buffer SimpleQueryStringQuery to convert
     * @return A configured SimpleQueryStringBuilder instance
     * @throws IllegalArgumentException if required fields are missing
     */
    static SimpleQueryStringBuilder fromProto(SimpleQueryStringQuery simpleQueryStringProto) {
        // Extract fields in the order they appear in fromXContent
        String queryBody = simpleQueryStringProto.getQuery();
        Map<String, Float> fieldsAndWeights = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzerName = null;
        Operator defaultOperator = null;
        int flags = SimpleQueryStringFlag.ALL.value();
        Boolean lenient = null;
        boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        String queryName = null;
        String minimumShouldMatch = null;
        String quoteFieldSuffix = null;
        boolean autoGenerateSynonymsPhraseQuery = true;
        int fuzzyPrefixLength = SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        int fuzzyMaxExpansions = SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        boolean fuzzyTranspositions = SimpleQueryStringBuilder.DEFAULT_FUZZY_TRANSPOSITIONS;

        // Query text is required
        if (queryBody == null || queryBody.isEmpty()) {
            throw new IllegalArgumentException(SimpleQueryStringBuilder.QUERY_TEXT_MISSING);
        }

        // Process fields (optional)
        if (simpleQueryStringProto.getFieldsCount() > 0) {
            fieldsAndWeights = new HashMap<>();
            for (String field : simpleQueryStringProto.getFieldsList()) {
                // Fields are stored as "field^boost" format or just "field"
                String[] fieldAndBoost = field.split("\\^");
                if (fieldAndBoost.length == 2) {
                    try {
                        float fieldBoost = Float.parseFloat(fieldAndBoost[1]);
                        fieldsAndWeights.put(fieldAndBoost[0], fieldBoost);
                    } catch (NumberFormatException e) {
                        fieldsAndWeights.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
                    }
                } else {
                    fieldsAndWeights.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
                }
            }
        }

        // Process boost (optional)
        if (simpleQueryStringProto.hasBoost()) {
            boost = simpleQueryStringProto.getBoost();
        }

        // Process analyzer (optional)
        if (simpleQueryStringProto.hasAnalyzer()) {
            analyzerName = simpleQueryStringProto.getAnalyzer();
        }

        // Process default_operator (optional)
        if (simpleQueryStringProto.hasDefaultOperator()
            && simpleQueryStringProto.getDefaultOperator() != org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED) {
            defaultOperator = OperatorProtoUtils.fromEnum(simpleQueryStringProto.getDefaultOperator());
        }

        // Process flags (optional)
        if (simpleQueryStringProto.hasFlags()) {
            flags = parseFlags(simpleQueryStringProto.getFlags());
        }

        // Process lenient (optional)
        if (simpleQueryStringProto.hasLenient()) {
            lenient = simpleQueryStringProto.getLenient();
        }

        // Process analyze_wildcard (optional)
        if (simpleQueryStringProto.hasAnalyzeWildcard()) {
            analyzeWildcard = simpleQueryStringProto.getAnalyzeWildcard();
        }

        // Process queryName (optional)
        if (simpleQueryStringProto.hasXName()) {
            queryName = simpleQueryStringProto.getXName();
        }

        // Process minimum_should_match (optional)
        if (simpleQueryStringProto.hasMinimumShouldMatch()) {
            switch (simpleQueryStringProto.getMinimumShouldMatch().getMinimumShouldMatchCase()) {
                case INT32:
                    minimumShouldMatch = String.valueOf(simpleQueryStringProto.getMinimumShouldMatch().getInt32());
                    break;
                case STRING:
                    minimumShouldMatch = simpleQueryStringProto.getMinimumShouldMatch().getString();
                    break;
                default:
                    // No minimum_should_match specified
                    break;
            }
        }

        // Process quote_field_suffix (optional)
        if (simpleQueryStringProto.hasQuoteFieldSuffix()) {
            quoteFieldSuffix = simpleQueryStringProto.getQuoteFieldSuffix();
        }

        // Process auto_generate_synonyms_phrase_query (optional)
        if (simpleQueryStringProto.hasAutoGenerateSynonymsPhraseQuery()) {
            autoGenerateSynonymsPhraseQuery = simpleQueryStringProto.getAutoGenerateSynonymsPhraseQuery();
        }

        // Process fuzzy_prefix_length (optional)
        if (simpleQueryStringProto.hasFuzzyPrefixLength()) {
            fuzzyPrefixLength = simpleQueryStringProto.getFuzzyPrefixLength();
        }

        // Process fuzzy_max_expansions (optional)
        if (simpleQueryStringProto.hasFuzzyMaxExpansions()) {
            fuzzyMaxExpansions = simpleQueryStringProto.getFuzzyMaxExpansions();
        }

        // Process fuzzy_transpositions (optional)
        if (simpleQueryStringProto.hasFuzzyTranspositions()) {
            fuzzyTranspositions = simpleQueryStringProto.getFuzzyTranspositions();
        }

        // Build the query in the same order as fromXContent
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder(queryBody);
        if (fieldsAndWeights != null) {
            qb.fields(fieldsAndWeights);
        }
        qb.boost(boost).analyzer(analyzerName).queryName(queryName).minimumShouldMatch(minimumShouldMatch);
        qb.flags(convertIntToFlags(flags)).defaultOperator(defaultOperator);
        if (lenient != null) {
            qb.lenient(lenient);
        }
        qb.analyzeWildcard(analyzeWildcard).quoteFieldSuffix(quoteFieldSuffix);
        qb.autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        qb.fuzzyPrefixLength(fuzzyPrefixLength);
        qb.fuzzyMaxExpansions(fuzzyMaxExpansions);
        qb.fuzzyTranspositions(fuzzyTranspositions);

        return qb;
    }

    /**
     * Converts an integer flags value to SimpleQueryStringFlag array.
     * This is needed because SimpleQueryStringBuilder.flags(int) is package-private.
     *
     * @param flagsValue The integer flags value
     * @return Array of SimpleQueryStringFlag
     */
    private static SimpleQueryStringFlag[] convertIntToFlags(int flagsValue) {
        if (flagsValue == -1) {
            return new SimpleQueryStringFlag[] { SimpleQueryStringFlag.ALL };
        }
        if (flagsValue == 0) {
            return new SimpleQueryStringFlag[] { SimpleQueryStringFlag.NONE };
        }

        // Decompose the flags value into individual flags
        java.util.List<SimpleQueryStringFlag> flagsList = new java.util.ArrayList<>();
        for (SimpleQueryStringFlag flag : SimpleQueryStringFlag.values()) {
            if (flag == SimpleQueryStringFlag.ALL || flag == SimpleQueryStringFlag.NONE) {
                continue; // Skip ALL and NONE as they're special cases
            }
            if ((flagsValue & flag.value()) != 0) {
                flagsList.add(flag);
            }
        }

        return flagsList.toArray(new SimpleQueryStringFlag[0]);
    }

    /**
     * Parses the flags from the Protocol Buffer format.
     * Handles both single flag and multiple flags (string format).
     *
     * @param flagsProto The Protocol Buffer SimpleQueryStringFlags
     * @return The integer flag value
     */
    private static int parseFlags(org.opensearch.protobufs.SimpleQueryStringFlags flagsProto) {
        switch (flagsProto.getSimpleQueryStringFlagsCase()) {
            case SINGLE:
                return convertSingleFlag(flagsProto.getSingle());
            case MULTIPLE:
                return resolveFlagsString(flagsProto.getMultiple());
            default:
                return SimpleQueryStringFlag.ALL.value();
        }
    }

    /**
     * Resolves flags from a pipe-delimited string (e.g., "AND|OR|NOT").
     * This is our own implementation since SimpleQueryStringFlag.resolveFlags is package-private.
     *
     * @param flags The pipe-delimited flags string
     * @return The integer flag value
     */
    private static int resolveFlagsString(String flags) {
        if (flags == null || flags.isEmpty()) {
            return SimpleQueryStringFlag.ALL.value();
        }

        int magic = SimpleQueryStringFlag.NONE.value();
        for (String s : flags.split("\\|")) {
            s = s.trim();
            if (s.isEmpty()) {
                continue;
            }
            try {
                SimpleQueryStringFlag flag = SimpleQueryStringFlag.valueOf(s.toUpperCase(java.util.Locale.ROOT));
                switch (flag) {
                    case NONE:
                        return 0;
                    case ALL:
                        return -1;
                    default:
                        magic |= flag.value();
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(SimpleQueryStringBuilder.UNKNOWN_FLAG_PREFIX + s + "]");
            }
        }
        return magic;
    }

    /**
     * Converts a single Protocol Buffer flag to the integer value.
     *
     * @param flag The Protocol Buffer SimpleQueryStringFlag
     * @return The integer flag value
     */
    private static int convertSingleFlag(org.opensearch.protobufs.SimpleQueryStringFlag flag) {
        switch (flag) {
            case SIMPLE_QUERY_STRING_FLAG_ALL:
                return SimpleQueryStringFlag.ALL.value();
            case SIMPLE_QUERY_STRING_FLAG_AND:
                return SimpleQueryStringFlag.AND.value();
            case SIMPLE_QUERY_STRING_FLAG_ESCAPE:
                return SimpleQueryStringFlag.ESCAPE.value();
            case SIMPLE_QUERY_STRING_FLAG_FUZZY:
                return SimpleQueryStringFlag.FUZZY.value();
            case SIMPLE_QUERY_STRING_FLAG_NEAR:
                return SimpleQueryStringFlag.NEAR.value();
            case SIMPLE_QUERY_STRING_FLAG_NONE:
                return SimpleQueryStringFlag.NONE.value();
            case SIMPLE_QUERY_STRING_FLAG_NOT:
                return SimpleQueryStringFlag.NOT.value();
            case SIMPLE_QUERY_STRING_FLAG_OR:
                return SimpleQueryStringFlag.OR.value();
            case SIMPLE_QUERY_STRING_FLAG_PHRASE:
                return SimpleQueryStringFlag.PHRASE.value();
            case SIMPLE_QUERY_STRING_FLAG_PRECEDENCE:
                return SimpleQueryStringFlag.PRECEDENCE.value();
            case SIMPLE_QUERY_STRING_FLAG_PREFIX:
                return SimpleQueryStringFlag.PREFIX.value();
            case SIMPLE_QUERY_STRING_FLAG_SLOP:
                return SimpleQueryStringFlag.SLOP.value();
            case SIMPLE_QUERY_STRING_FLAG_WHITESPACE:
                return SimpleQueryStringFlag.WHITESPACE.value();
            default:
                return SimpleQueryStringFlag.ALL.value();
        }
    }
}
