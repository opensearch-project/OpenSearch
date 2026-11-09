/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

class NativeArrowStreamDataRequest extends ActionRequest {
    private final int batchCount;
    private final int rowsPerBatch;

    NativeArrowStreamDataRequest(int batchCount, int rowsPerBatch) {
        this.batchCount = batchCount;
        this.rowsPerBatch = rowsPerBatch;
    }

    NativeArrowStreamDataRequest(StreamInput in) throws IOException {
        super(in);
        this.batchCount = in.readInt();
        this.rowsPerBatch = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(batchCount);
        out.writeInt(rowsPerBatch);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public int getRowsPerBatch() {
        return rowsPerBatch;
    }
}
