/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.Rescore;
import org.opensearch.search.rescore.RescorerBuilder;

/**
 * Utility class for converting Rescore Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of rescorers
 * into their corresponding OpenSearch RescorerBuilder implementations for search result rescoring.
 */
public class RescorerBuilderProtoUtils {

    private RescorerBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer Rescore to an OpenSearch RescorerBuilder.
     * Similar to {@link RescorerBuilder#parseFromXContent(XContentParser)}, this method
     * would parse the Protocol Buffer representation and create a properly configured
     * RescorerBuilder with the appropriate settings.
     *
     * @param rescoreProto The Protocol Buffer Rescore to convert
     * @return A configured RescorerBuilder instance
     * @throws UnsupportedOperationException as rescore functionality is not yet implemented
     */
    protected static RescorerBuilder<?> parseFromProto(Rescore rescoreProto) {
        throw new UnsupportedOperationException("rescore is not supported yet");
        /*
        RescorerBuilder<?> rescorer = null;
        // TODO populate rescorerBuilder

        return rescorer;

        */
    }

}
