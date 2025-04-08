/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.Highlight;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

/**
 * Utility class for converting Highlight Protocol Buffers to objects
 *
 */
public class HighlightBuilderProtoUtils {

    private HighlightBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link HighlightBuilder#fromXContent(XContentParser)}
     *
     * @param highlightProto
     */

    public static HighlightBuilder fromProto(Highlight highlightProto) {

        throw new UnsupportedOperationException("highlight not supported yet");

        /*
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // TODO populate highlightBuilder
        return highlightBuilder;
        */

    }

}
