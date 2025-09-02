/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.Highlight;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

/**
 * Utility class for converting Highlight Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of highlights
 * into their corresponding OpenSearch HighlightBuilder implementations for search result highlighting.
 */
public class HighlightBuilderProtoUtils {

    private HighlightBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer Highlight to an OpenSearch HighlightBuilder.
     * Similar to {@link HighlightBuilder#fromXContent(XContentParser)}, this method
     * would parse the Protocol Buffer representation and create a properly configured
     * HighlightBuilder with the appropriate settings.
     *
     * @param highlightProto The Protocol Buffer Highlight to convert
     * @return A configured HighlightBuilder instance
     * @throws UnsupportedOperationException as highlight functionality is not yet implemented
     */
    protected static HighlightBuilder fromProto(Highlight highlightProto) {

        throw new UnsupportedOperationException("highlight not supported yet");

        /*
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // TODO populate highlightBuilder
        return highlightBuilder;
        */
    }
}
