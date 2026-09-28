/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/** Per-node request carrying which caches to clear. @opensearch.internal */
public class ClearCacheNodeRequest extends TransportRequest {

    private boolean footer;
    private boolean column;
    private boolean offset;
    private boolean statistics;

    public ClearCacheNodeRequest(boolean footer, boolean column, boolean offset, boolean statistics) {
        this.footer = footer;
        this.column = column;
        this.offset = offset;
        this.statistics = statistics;
    }

    public ClearCacheNodeRequest(StreamInput in) throws IOException {
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

    public boolean isFooter() {
        return footer;
    }

    public boolean isColumn() {
        return column;
    }

    public boolean isOffset() {
        return offset;
    }

    public boolean isStatistics() {
        return statistics;
    }

    public boolean isClearAll() {
        return !footer && !column && !offset && !statistics;
    }
}
