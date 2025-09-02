/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.suggest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.Suggester;
import org.opensearch.search.suggest.SuggestBuilder;

/**
 * Utility class for converting Highlight Protocol Buffers to objects
 *
 */
public class SuggestBuilderProtoUtils {

    private SuggestBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link SuggestBuilder#fromXContent(XContentParser)}
     *
     * @param suggesterProto
     */

    public static SuggestBuilder fromProto(Suggester suggesterProto) {
        SuggestBuilder suggestBuilder = new SuggestBuilder();

        // TODO populate suggestBuilder

        return suggestBuilder;
    }
}
