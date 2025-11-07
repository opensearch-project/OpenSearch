/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.ScriptScoreFunction;
import org.opensearch.script.Script;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;

/**
 * Utility class for converting Protocol Buffer ScriptScoreFunction to OpenSearch objects.
 * This utility converts Protocol Buffer ScriptScoreFunction objects into OpenSearch ScriptScoreFunctionBuilder instances.
 */
class ScriptScoreFunctionProtoUtils {

    private ScriptScoreFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ScriptScoreFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the script parameter and constructs the builder.
     *
     * @param scriptScore the Protocol Buffer ScriptScoreFunction
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the scriptScore is null or doesn't contain a script
     */
    static ScoreFunctionBuilder<?> fromProto(ScriptScoreFunction scriptScore) {
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
