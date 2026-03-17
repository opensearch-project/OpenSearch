/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Interface for merging multiple writer file sets into a single merged result.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Merger {
    /**
     * Merges a list of writer file sets into a single merged result.
     *
     * @param mergeInput input containing files to merge, and any instructions about how to execute the merge.
     * @return merge result containing row ID mapping and merged file metadata
     */
    MergeResult merge(MergeInput mergeInput) throws IOException;
}
