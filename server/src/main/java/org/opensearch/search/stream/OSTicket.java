/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A ticket for a stream.
 */
@ExperimentalApi
public class OSTicket implements Writeable, ToXContentFragment {

    private final byte[] bytes;

    public OSTicket(byte[] bytes) {
        this.bytes = bytes;
    }

    public OSTicket(StreamInput in) throws IOException {
        bytes = in.readByteArray();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(bytes);
    }

    @Override
    public String toString() {
        return "OSTicket{" + new String(bytes, StandardCharsets.UTF_8) + "}";
    }
}
