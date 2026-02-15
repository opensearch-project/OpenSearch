/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public record BytesValue(BytesReference bytes) implements ExtraFieldValue {
    @Override
    public Type type() {
        return Type.BYTES;
    }

    @Override
    public int size() {
        return bytes.length();
    }

    @Override
    public void writeBodyTo(StreamOutput out) throws IOException {
        out.writeBytesReference(bytes);
    }

    static BytesValue readBodyFrom(StreamInput in) throws IOException {
        return new BytesValue(in.readBytesReference());
    }
}
