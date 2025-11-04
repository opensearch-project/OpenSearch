/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.ScriptScoreFunction;
import org.opensearch.script.Script;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;

/**
 * Protocol Buffer converter for ScriptScoreFunction.
 * This converter handles the transformation of Protocol Buffer ScriptScoreFunction objects
 * into OpenSearch ScriptScoreFunctionBuilder instances.
 */
public class ScriptScoreFunctionProtoConverter {

    /**
     * Default constructor for ScriptScoreFunctionProtoConverter.
     */
    public ScriptScoreFunctionProtoConverter() {
        // Default constructor
    }

    /**
     * Returns the function score container case that this converter handles.
     *
     * @return the SCRIPT_SCORE function score container case
     */
    public FunctionScoreContainer.FunctionScoreContainerCase getHandledFunctionCase() {
        return FunctionScoreContainer.FunctionScoreContainerCase.SCRIPT_SCORE;
    }

    /**
     * Converts a Protocol Buffer FunctionScoreContainer containing a script score function
     * to an OpenSearch ScoreFunctionBuilder.
     *
     * @param container the Protocol Buffer FunctionScoreContainer containing the script score function
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the container is null or doesn't contain a SCRIPT_SCORE function
     */
    public ScoreFunctionBuilder<?> fromProto(FunctionScoreContainer container) {
        if (container == null
            || container.getFunctionScoreContainerCase() != FunctionScoreContainer.FunctionScoreContainerCase.SCRIPT_SCORE) {
            throw new IllegalArgumentException("FunctionScoreContainer must contain a ScriptScoreFunction");
        }

        return parseScriptScoreFunction(container.getScriptScore());
    }

    /**
     * Parses a ScriptScoreFunction and creates a ScriptScoreFunctionBuilder.
     *
     * @param scriptScore the protobuf ScriptScoreFunction
     * @return the corresponding OpenSearch ScriptScoreFunctionBuilder
     */
    private static ScoreFunctionBuilder<?> parseScriptScoreFunction(ScriptScoreFunction scriptScore) {
        if (scriptScore == null) {
            throw new IllegalArgumentException("ScriptScoreFunction cannot be null");
        }

        if (!scriptScore.hasScript()) {
            throw new IllegalArgumentException("ScriptScoreFunction must have a script");
        }

        // Convert protobuf Script to OpenSearch Script using existing utility
        Script script = ScriptProtoUtils.parseFromProtoRequest(scriptScore.getScript());

        // Create and return ScriptScoreFunctionBuilder
        return new org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder(script);
    }
}
