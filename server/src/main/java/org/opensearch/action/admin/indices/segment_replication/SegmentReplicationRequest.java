/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SegmentReplicationRequest extends BroadcastRequest<SegmentReplicationRequest> {
    private boolean detailed = false;       // Provides extra details in the response
    private boolean activeOnly = false;     // Only reports on active recoveries

    /**
     * Constructs a request for segment replication information for all shards
     */
    public SegmentReplicationRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public SegmentReplicationRequest(StreamInput in) throws IOException {
        super(in);
        detailed = in.readBoolean();
        activeOnly = in.readBoolean();
    }

    /**
     * Constructs a request for segment replication information for all shards for the given indices
     *
     * @param indices   Comma-separated list of indices about which to gather segment replication information
     */
    public SegmentReplicationRequest(String... indices) {
        super(indices, IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);
    }

    /**
     * True if detailed flag is set, false otherwise. This value if false by default.
     *
     * @return  True if detailed flag is set, false otherwise
     */
    public boolean detailed() {
        return detailed;
    }

    /**
     * Set value of the detailed flag. Detailed requests will contain extra
     * information.
     *
     * @param detailed  Whether or not to set the detailed flag
     */
    public void detailed(boolean detailed) {
        this.detailed = detailed;
    }

    /**
     * True if activeOnly flag is set, false otherwise. This value is false by default.
     *
     * @return  True if activeOnly flag is set, false otherwise
     */
    public boolean activeOnly() {
        return activeOnly;
    }

    /**
     * Set value of the activeOnly flag. If true, this request will only response with
     * on-going recovery information.
     *
     * @param activeOnly    Whether or not to set the activeOnly flag.
     */
    public void activeOnly(boolean activeOnly) {
        this.activeOnly = activeOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(activeOnly);
    }
}
