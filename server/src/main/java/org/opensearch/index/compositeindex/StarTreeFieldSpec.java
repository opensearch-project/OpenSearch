/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Star tree index specific settings for a composite field.
 */
@ExperimentalApi
public class StarTreeFieldSpec implements CompositeFieldSpec {

    private final AtomicInteger maxLeafDocs = new AtomicInteger();
    private final List<String> skipStarNodeCreationInDims;
    private final StarTreeBuildMode buildMode;

    public StarTreeFieldSpec(int maxLeafDocs, List<String> skipStarNodeCreationInDims, StarTreeBuildMode buildMode) {
        this.maxLeafDocs.set(maxLeafDocs);
        this.skipStarNodeCreationInDims = skipStarNodeCreationInDims;
        this.buildMode = buildMode;
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

    public int maxLeafDocs() {
        return maxLeafDocs.get();
    }

    @Override
    public void setDefaults(CompositeIndexSettings compositeIndexSettings) {
        if (maxLeafDocs.get() == Integer.MAX_VALUE) {
            maxLeafDocs.set(compositeIndexSettings.getMaxLeafDocs());
        }
    }
}
