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

class StreamDataRequest extends ActionRequest {
    private int count = 10;
    private long delayMs = 1000;

    public StreamDataRequest() {}

    public StreamDataRequest(StreamInput in) throws IOException {
        super(in);
        count = in.readInt();
        delayMs = in.readLong();
    }

    public StreamDataRequest(int count, long delayMs) {
        this.count = count;
        this.delayMs = delayMs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(count);
        out.writeLong(delayMs);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public int getCount() {
        return count;
    }

    public long getDelayMs() {
        return delayMs;
    }
}
