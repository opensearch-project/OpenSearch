/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.RandomScoreFunction;

/**
 * Utility class for converting Protocol Buffer RandomScoreFunction to OpenSearch objects.
 * This utility handles the transformation of Protocol Buffer RandomScoreFunction objects
 * into OpenSearch RandomScoreFunctionBuilder instances.
 */
class RandomScoreFunctionProtoUtils {

    private RandomScoreFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer RandomScoreFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link RandomScoreFunctionBuilder#fromXContent(XContentParser)}, this method
     * parses the seed and optional field parameters.
     *
     * @param randomScore the Protocol Buffer RandomScoreFunction
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the randomScore is null
     */
    static ScoreFunctionBuilder<?> fromProto(RandomScoreFunction randomScore) {
        if (randomScore == null) {
            throw new IllegalArgumentException("RandomScoreFunction cannot be null");
        }

        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();

        // Set field if present
        if (!randomScore.getField().isEmpty()) {
            builder.setField(randomScore.getField());
        }

        // Set seed if present
        if (randomScore.hasSeed()) {
            org.opensearch.protobufs.RandomScoreFunctionSeed seed = randomScore.getSeed();
            if (seed.hasInt32()) {
                builder.seed(seed.getInt32());
            } else if (seed.hasInt64()) {
                builder.seed(seed.getInt64());
            } else if (seed.hasString()) {
                builder.seed(seed.getString());
            }
        }

        return builder;
    }
}
