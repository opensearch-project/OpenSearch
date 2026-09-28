/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Cluster-level request to clear DataFusion caches on target nodes.
 *
 * <p>When all flags are false (the default), every cache is cleared. Set individual
 * flags to clear only specific caches — mirrors {@code /_cache/clear?query=true} pattern.
 *
 * @opensearch.internal
 */
public class ClearCacheNodesRequest extends BaseNodesRequest<ClearCacheNodesRequest> {

    private boolean footer;
    private boolean column;
    private boolean offset;
    private boolean statistics;

    /** Clears ALL caches on the target nodes (no params given). */
    public ClearCacheNodesRequest(String... nodesIds) {
        super(nodesIds);
    }

    public ClearCacheNodesRequest(StreamInput in) throws IOException {
        super(in);
        this.footer = in.readBoolean();
        this.column = in.readBoolean();
        this.offset = in.readBoolean();
        this.statistics = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(footer);
        out.writeBoolean(column);
        out.writeBoolean(offset);
        out.writeBoolean(statistics);
    }

    /** Whether to clear the footer metadata cache. */
    public boolean isFooter() {
        return footer;
    }

    public void setFooter(boolean footer) {
        this.footer = footer;
    }

    /** Whether to clear the ColumnIndex (predicate) cache. */
    public boolean isColumn() {
        return column;
    }

    public void setColumn(boolean column) {
        this.column = column;
    }

    /** Whether to clear the OffsetIndex (projection) cache. */
    public boolean isOffset() {
        return offset;
    }

    public void setOffset(boolean offset) {
        this.offset = offset;
    }

    public boolean isStatistics() {
        return statistics;
    }

    public void setStatistics(boolean statistics) {
        this.statistics = statistics;
    }

    /** True when no specific flag is set — means clear everything. */
    public boolean isClearAll() {
        return !footer && !column && !offset && !statistics;
    }
}
