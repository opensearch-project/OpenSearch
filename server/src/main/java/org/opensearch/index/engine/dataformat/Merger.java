/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.List;

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
     * @param fileMetadataList list of writer file sets to merge
     * @param writerGeneration the writer generation number
     * @return merge result containing row ID mapping and merged file metadata
     */
    MergeResult merge(List<WriterFileSet> fileMetadataList, long writerGeneration);
}
