/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.Rescore;
import org.opensearch.search.rescore.RescorerBuilder;

/**
 * Utility class for converting Rescore Protocol Buffers to objects
 *
 */
public class RescorerBuilderProtoUtils {

    private RescorerBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link RescorerBuilder#parseFromXContent(XContentParser)} (XContentParser)}
     *
     * @param rescoreProto
     */

    public static RescorerBuilder<?> parseFromProto(Rescore rescoreProto) {
        throw new UnsupportedOperationException("rescore is not supported yet");
        /*
        RescorerBuilder<?> rescorer = null;
        // TODO populate rescorerBuilder

        return rescorer;

        */
    }

}
