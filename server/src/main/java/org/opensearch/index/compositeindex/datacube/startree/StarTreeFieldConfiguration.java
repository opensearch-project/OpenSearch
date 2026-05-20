/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Star tree index specific configuration
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeFieldConfiguration implements ToXContent {

    private final AtomicInteger maxLeafDocs = new AtomicInteger();
    private final Set<String> skipStarNodeCreationInDims;
    private final StarTreeBuildMode buildMode;

    public StarTreeFieldConfiguration(int maxLeafDocs, Set<String> skipStarNodeCreationInDims, StarTreeBuildMode buildMode) {
        this.maxLeafDocs.set(maxLeafDocs);
        this.skipStarNodeCreationInDims = skipStarNodeCreationInDims;
        this.buildMode = buildMode;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // build mode is internal and not part of user mappings config, hence not added as part of toXContent
        builder.field("max_leaf_docs", maxLeafDocs.get());
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
        // TODO : remove onheap support unless this proves useful
        ON_HEAP("onheap", (byte) 0),
        OFF_HEAP("offheap", (byte) 1);

        private final String typeName;
        private final byte buildModeOrdinal;

        StarTreeBuildMode(String typeName, byte buildModeOrdinal) {
            this.typeName = typeName;
            this.buildModeOrdinal = buildModeOrdinal;
        }

        public String getTypeName() {
            return typeName;
        }

        public byte getBuildModeOrdinal() {
            return buildModeOrdinal;
        }

        public static StarTreeBuildMode fromTypeName(String typeName) {
            for (StarTreeBuildMode starTreeBuildMode : StarTreeBuildMode.values()) {
                if (starTreeBuildMode.getTypeName().equalsIgnoreCase(typeName)) {
                    return starTreeBuildMode;
                }
            }
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid star tree build mode: [%s] ", typeName));
        }

        public static StarTreeBuildMode fromBuildModeOrdinal(byte buildModeOrdinal) {
            for (StarTreeBuildMode starTreeBuildMode : StarTreeBuildMode.values()) {
                if (starTreeBuildMode.getBuildModeOrdinal() == buildModeOrdinal) {
                    return starTreeBuildMode;
                }
            }
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid star tree build mode: [%s] ", buildModeOrdinal));
        }

    }

    public int maxLeafDocs() {
        return maxLeafDocs.get();
    }

    public StarTreeBuildMode getBuildMode() {
        return buildMode;
    }

    public Set<String> getSkipStarNodeCreationInDims() {
        return skipStarNodeCreationInDims;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarTreeFieldConfiguration that = (StarTreeFieldConfiguration) o;
        return Objects.equals(maxLeafDocs.get(), that.maxLeafDocs.get())
            && Objects.equals(skipStarNodeCreationInDims, that.skipStarNodeCreationInDims)
            && buildMode == that.buildMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxLeafDocs.get(), skipStarNodeCreationInDims, buildMode);
    }
}
