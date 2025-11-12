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
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.protobufs.FuzzyQuery;
import org.opensearch.protobufs.MultiTermQueryRewrite;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting FuzzyQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of fuzzy queries
 * into their corresponding OpenSearch FuzzyQueryBuilder implementations for search operations.
 */
class FuzzyQueryBuilderProtoUtils {

    private FuzzyQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FuzzyQuery to an OpenSearch FuzzyQueryBuilder.
     * Similar to {@link FuzzyQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * FuzzyQueryBuilder with the appropriate field name, value, fuzziness, prefix length,
     * max expansions, transpositions, rewrite method, boost, and query name.
     *
     * @param fuzzyQueryProto The Protocol Buffer FuzzyQuery object
     * @return A configured FuzzyQueryBuilder instance
     * @throws IllegalArgumentException if the field name or value is null or empty
     */
    static FuzzyQueryBuilder fromProto(FuzzyQuery fuzzyQueryProto) {
        String fieldName = fuzzyQueryProto.getField();
        Object value = FieldValueProtoUtils.fromProto(fuzzyQueryProto.getValue(), false);
        Fuzziness fuzziness = FuzzyQueryBuilder.DEFAULT_FUZZINESS;
        int prefixLength = FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH;
        int maxExpansions = FuzzyQueryBuilder.DEFAULT_MAX_EXPANSIONS;
        boolean transpositions = FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS;
        String rewrite = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        if (fuzzyQueryProto.hasBoost()) {
            boost = fuzzyQueryProto.getBoost();
        }

        if (fuzzyQueryProto.hasFuzziness()) {
            org.opensearch.protobufs.Fuzziness fuzzinessProto = fuzzyQueryProto.getFuzziness();
            if (fuzzinessProto.hasString()) {
                fuzziness = Fuzziness.build(fuzzinessProto.getString());
            } else if (fuzzinessProto.hasInt32()) {
                fuzziness = Fuzziness.fromEdits(fuzzinessProto.getInt32());
            }
        }

        if (fuzzyQueryProto.hasPrefixLength()) {
            prefixLength = fuzzyQueryProto.getPrefixLength();
        }

        if (fuzzyQueryProto.hasMaxExpansions()) {
            maxExpansions = fuzzyQueryProto.getMaxExpansions();
        }

        if (fuzzyQueryProto.hasTranspositions()) {
            transpositions = fuzzyQueryProto.getTranspositions();
        }

        if (fuzzyQueryProto.hasRewrite()) {
            MultiTermQueryRewrite rewriteEnum = fuzzyQueryProto.getRewrite();
            if (rewriteEnum != MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED) {
                rewrite = ProtobufEnumUtils.convertToString(rewriteEnum);
            }
        }

        if (fuzzyQueryProto.hasXName()) {
            queryName = fuzzyQueryProto.getXName();
        }

        return new FuzzyQueryBuilder(fieldName, value).fuzziness(fuzziness)
            .prefixLength(prefixLength)
            .maxExpansions(maxExpansions)
            .transpositions(transpositions)
            .rewrite(rewrite)
            .boost(boost)
            .queryName(queryName);
    }
}
