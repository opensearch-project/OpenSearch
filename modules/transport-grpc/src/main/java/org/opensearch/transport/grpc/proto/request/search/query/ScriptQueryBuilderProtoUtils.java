/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.protobufs.ScriptQuery;
import org.opensearch.script.Script;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;

/**
 * Utility class for converting ScriptQuery Protocol Buffers to ScriptQueryBuilder objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch query builder objects.
 */
class ScriptQueryBuilderProtoUtils {

    private ScriptQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ScriptQuery Protocol Buffer to a ScriptQueryBuilder object.
     * This method follows the same pattern as ScriptQueryBuilder.fromXContent().
     *
     * @param scriptQueryProto the Protocol Buffer ScriptQuery to convert
     * @return the converted ScriptQueryBuilder object
     * @throws IllegalArgumentException if the script query proto is null or invalid
     */
    static ScriptQueryBuilder fromProto(ScriptQuery scriptQueryProto) {
        if (scriptQueryProto == null) {
            throw new IllegalArgumentException("ScriptQuery cannot be null");
        }

        if (!scriptQueryProto.hasScript()) {
            throw new IllegalArgumentException("script must be provided with a [script] query");
        }

        Script script = ScriptProtoUtils.parseFromProtoRequest(scriptQueryProto.getScript());

        float boost = ScriptQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        if (scriptQueryProto.hasBoost()) {
            boost = scriptQueryProto.getBoost();
        }

        if (scriptQueryProto.hasXName()) {
            queryName = scriptQueryProto.getXName();
        }

        return new ScriptQueryBuilder(script).boost(boost).queryName(queryName);
    }
}
