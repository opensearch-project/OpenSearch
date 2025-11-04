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
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.RandomScoreFunction;

/**
 * Protocol Buffer converter for RandomScoreFunction.
 * This converter handles the transformation of Protocol Buffer RandomScoreFunction objects
 * into OpenSearch RandomScoreFunctionBuilder instances.
 */
public class RandomScoreFunctionProtoConverter {

    /**
     * Default constructor for RandomScoreFunctionProtoConverter.
     */
    public RandomScoreFunctionProtoConverter() {
        // Default constructor
    }

    /**
     * Returns the function score container case that this converter handles.
     *
     * @return the RANDOM_SCORE function score container case
     */
    public FunctionScoreContainer.FunctionScoreContainerCase getHandledFunctionCase() {
        return FunctionScoreContainer.FunctionScoreContainerCase.RANDOM_SCORE;
    }

    /**
     * Converts a Protocol Buffer FunctionScoreContainer containing a random score function
     * to an OpenSearch ScoreFunctionBuilder.
     *
     * @param container the Protocol Buffer FunctionScoreContainer containing the random score function
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the container is null or doesn't contain a RANDOM_SCORE function
     */
    public ScoreFunctionBuilder<?> fromProto(FunctionScoreContainer container) {
        if (container == null
            || container.getFunctionScoreContainerCase() != FunctionScoreContainer.FunctionScoreContainerCase.RANDOM_SCORE) {
            throw new IllegalArgumentException("FunctionScoreContainer must contain a RandomScoreFunction");
        }

        return parseRandomScoreFunction(container.getRandomScore());
    }

    /**
     * Parses a RandomScoreFunction and creates a RandomScoreFunctionBuilder.
     *
     * @param randomScore the protobuf RandomScoreFunction
     * @return the corresponding OpenSearch RandomScoreFunctionBuilder
     */
    private static RandomScoreFunctionBuilder parseRandomScoreFunction(RandomScoreFunction randomScore) {
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
            } else if (seed.hasString()) {
                builder.seed(seed.getString());
            }
        }

        return builder;
    }
}
