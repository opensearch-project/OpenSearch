/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport response carrying the output of a shard fragment execution as an
 * Arrow IPC stream payload (schema header + zero or more record-batch messages,
 * produced by {@link org.apache.arrow.vector.ipc.ArrowStreamWriter}).
 *
 * <p>Arrow IPC handles every Arrow type natively (temporal, string-view,
 * dictionary, nested) without hand-rolled per-type serialization. Previously,
 * this response carried {@code List<Object[]>} rows and relied on
 * {@code StreamOutput.writeGenericValue} — which does not support Java 8+
 * temporal types like {@link java.time.LocalDateTime} and so failed the moment
 * a shard emitted a batch with a Timestamp column.
 *
 * <p>Wire format: {@code ipcPayload (byte[]) + rowCount (vint)}. The row count
 * is the total across all batches in the payload, cached for metrics / logging
 * so consumers don't have to decode the payload just to report "N rows handled".
 *
 * @opensearch.internal
 */
public class FragmentExecutionResponse extends ActionResponse {

    private final byte[] ipcPayload;
    private final int rowCount;

    public FragmentExecutionResponse(byte[] ipcPayload, int rowCount) {
        this.ipcPayload = ipcPayload;
        this.rowCount = rowCount;
    }

    public FragmentExecutionResponse(StreamInput in) throws IOException {
        super(in);
        this.ipcPayload = in.readByteArray();
        this.rowCount = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(ipcPayload);
        out.writeVInt(rowCount);
    }

    /**
     * Arrow IPC stream bytes — a schema message followed by zero or more record
     * batch messages, as written by {@link org.apache.arrow.vector.ipc.ArrowStreamWriter}.
     * An empty array means the fragment produced no output at all (no schema,
     * no rows).
     */
    public byte[] getIpcPayload() {
        return ipcPayload;
    }

    /**
     * Total number of rows across all batches in {@link #getIpcPayload()}.
     */
    public int getRowCount() {
        return rowCount;
    }
}
