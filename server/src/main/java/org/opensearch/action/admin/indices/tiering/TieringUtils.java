/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for tiering operations
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieringUtils {

    /**
     * Constructs a HotToWarmTieringResponse from the rejected indices map
     *
     * @param rejectedIndices the rejected indices map
     * @return the HotToWarmTieringResponse object
     */
    public static HotToWarmTieringResponse constructToHotToWarmTieringResponse(final Map<Index, String> rejectedIndices) {
        final List<HotToWarmTieringResponse.IndexResult> indicesResult = new LinkedList<>();
        for (Map.Entry<Index, String> rejectedIndex : rejectedIndices.entrySet()) {
            indicesResult.add(new HotToWarmTieringResponse.IndexResult(rejectedIndex.getKey().getName(), rejectedIndex.getValue()));
        }
        return new HotToWarmTieringResponse(true, indicesResult);
    }
}
