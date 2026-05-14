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
 * Transport response for QTF fetch phase.
 * Carries Arrow IPC stream bytes containing the fetched rows with __row_id__.
 */
public class FetchByRowIdsResponse extends ActionResponse {

    private final byte[] ipcPayload;
    private final int rowCount;

    public FetchByRowIdsResponse(byte[] ipcPayload, int rowCount) {
        this.ipcPayload = ipcPayload;
        this.rowCount = rowCount;
    }

    public FetchByRowIdsResponse(StreamInput in) throws IOException {
        super(in);
        this.ipcPayload = in.readByteArray();
        this.rowCount = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(ipcPayload);
        out.writeVInt(rowCount);
    }

    public byte[] getIpcPayload() {
        return ipcPayload;
    }

    public int getRowCount() {
        return rowCount;
    }
}
