/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.protobufs.FieldCollapse;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting CollapseBuilder Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of field collapse
 * specifications into their corresponding OpenSearch CollapseBuilder implementations for
 * search result field collapsing and grouping.
 */
public class CollapseBuilderProtoUtils {

    private CollapseBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FieldCollapse to an OpenSearch CollapseBuilder.
     * Similar to {@link CollapseBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * CollapseBuilder with the appropriate field, max concurrent group searches,
     * and inner hits settings.
     *
     * @param collapseProto The Protocol Buffer FieldCollapse to convert
     * @param registry The registry for query conversion (needed for inner hits with sorts/highlights)
     * @return A configured CollapseBuilder instance
     * @throws IOException if there's an error during parsing or conversion
     */
    static CollapseBuilder fromProto(FieldCollapse collapseProto, QueryBuilderProtoConverterRegistry registry) throws IOException {
        CollapseBuilder collapseBuilder = new CollapseBuilder(collapseProto.getField());

        if (collapseProto.hasMaxConcurrentGroupSearches()) {
            collapseBuilder.setMaxConcurrentGroupRequests(collapseProto.getMaxConcurrentGroupSearches());
        }
        if (collapseProto.getInnerHitsCount() > 0) {
            List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
            for (org.opensearch.protobufs.InnerHits innerHits : collapseProto.getInnerHitsList()) {
                innerHitBuilders.add(InnerHitsBuilderProtoUtils.fromProto(innerHits, registry));
            }
            collapseBuilder.setInnerHits(innerHitBuilders);
        }

        return collapseBuilder;
    }
}
