/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Star tree index specific configuration
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeFieldSpec implements ToXContent {

    private final AtomicInteger maxLeafDocs = new AtomicInteger();
    private final List<String> skipStarNodeCreationInDims;
    private final StarTreeBuildMode buildMode;

    public StarTreeFieldSpec(int maxLeafDocs, List<String> skipStarNodeCreationInDims, StarTreeBuildMode buildMode) {
        this.maxLeafDocs.set(maxLeafDocs);
        this.skipStarNodeCreationInDims = skipStarNodeCreationInDims;
        this.buildMode = buildMode;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("max_leaf_docs", maxLeafDocs.get());
        builder.field("build_mode", buildMode.getTypeName());
        builder.startArray("skip_star_node_creation_for_dimensions");
        for (String dim : skipStarNodeCreationInDims) {
            builder.value(dim);
        }
        builder.endArray();
        return builder;
    }

    /**
     * Star tree build mode using which sorting and aggregations are performed during index creation.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum StarTreeBuildMode {
        ON_HEAP("onheap"),
        OFF_HEAP("offheap");

        private final String typeName;

        StarTreeBuildMode(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static StarTreeBuildMode fromTypeName(String typeName) {
            for (StarTreeBuildMode starTreeBuildMode : StarTreeBuildMode.values()) {
                if (starTreeBuildMode.getTypeName().equalsIgnoreCase(typeName)) {
                    return starTreeBuildMode;
                }
            }
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid star tree build mode: [%s] ", typeName));
        }
    }

    @Override
    public String toString() {
        return buildMode.getTypeName();
    }

    public int maxLeafDocs() {
        return maxLeafDocs.get();
    }

    public StarTreeBuildMode getBuildMode() {
        return buildMode;
    }

    public List<String> getSkipStarNodeCreationInDims() {
        return skipStarNodeCreationInDims;
    }
}
