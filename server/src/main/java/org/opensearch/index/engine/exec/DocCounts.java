/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Live and deleted document counts for one writer generation.
 * {@code liveDocs + deletedDocs} equals the number of rows physically present in the
 * generation, which is the value {@link WriterFileSet#numRows()} reports.
 *
 * @param liveDocs    documents still reachable in this generation
 * @param deletedDocs rows hidden by a delete, or by an update that superseded them
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record DocCounts(long liveDocs, long deletedDocs) {

    public DocCounts {
        if (liveDocs < 0) {
            throw new IllegalArgumentException("liveDocs must not be negative but was [" + liveDocs + "]");
        }
        if (deletedDocs < 0) {
            throw new IllegalArgumentException("deletedDocs must not be negative but was [" + deletedDocs + "]");
        }
    }

    public DocCounts plus(DocCounts other) {
        return new DocCounts(liveDocs + other.liveDocs, deletedDocs + other.deletedDocs);
    }

    public static DocCounts allLive(long rows) {
        return new DocCounts(rows, 0L);
    }
}
