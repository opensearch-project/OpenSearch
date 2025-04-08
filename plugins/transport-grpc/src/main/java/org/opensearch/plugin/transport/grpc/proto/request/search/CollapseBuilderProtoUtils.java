/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.FieldCollapse;
import org.opensearch.search.collapse.CollapseBuilder;

import java.io.IOException;

/**
 * Utility class for converting CollapseBuilder Protocol Buffers to objects
 *
 */
public class CollapseBuilderProtoUtils {

    private CollapseBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link CollapseBuilder#fromXContent(XContentParser)}
     *
     * @param collapseProto
     */

    public static CollapseBuilder fromProto(FieldCollapse collapseProto) throws IOException {
        CollapseBuilder collapseBuilder = new CollapseBuilder(collapseProto.getField());

        if (collapseProto.hasMaxConcurrentGroupSearches()) {
            collapseBuilder.setMaxConcurrentGroupRequests(collapseProto.getMaxConcurrentGroupSearches());
        }
        if (collapseProto.getInnerHitsCount() > 0) {
            collapseBuilder.setInnerHits(InnerHitsBuilderProtoUtils.fromProto(collapseProto.getInnerHitsList()));
        }

        return collapseBuilder;
    }

}
