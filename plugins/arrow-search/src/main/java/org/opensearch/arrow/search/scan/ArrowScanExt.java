/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.arrow.search.Column;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Marks a query phase as a shard scan, so the scan's collectors replace the top-docs collector.
 * <p>
 * It exists only on the data node that runs the scan. It is never parsed from or written to a request, so a search cannot
 * opt into a scan by itself.
 */
final class ArrowScanExt extends SearchExtBuilder {

    static final String NAME = "arrow_scan";

    private final ScanState state;

    ArrowScanExt(ScanState state) {
        this.state = state;
    }

    ScanState state() {
        return state;
    }

    @Override
    public Executor sliceExecutor() {
        // Slices wait for the coordinator while they send, so they run on virtual threads, not on the index_searcher pool.
        return state.sliceExecutor;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("[" + NAME + "] is local to the node running the scan");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(NAME).field("columns", state.columns.stream().map(Column::name).toList()).endObject();
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(state);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ArrowScanExt other && other.state == state;
    }
}
