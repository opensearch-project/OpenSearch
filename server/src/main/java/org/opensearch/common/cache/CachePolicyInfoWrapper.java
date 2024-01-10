/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A class containing information needed for all CacheTierPolicy objects to decide whether to admit
 * a given BytesReference. This spares us from having to create an entire short-lived QuerySearchResult object
 * just to read a few values.
 */
public class CachePolicyInfoWrapper implements Writeable {
    private final Long tookTimeNanos;

    public CachePolicyInfoWrapper(Long tookTimeNanos) {
        this.tookTimeNanos = tookTimeNanos;
        // Add more values here as they are needed for future cache tier policies
    }

    public CachePolicyInfoWrapper(StreamInput in) throws IOException {
        this.tookTimeNanos = in.readOptionalLong();
    }

    public Long getTookTimeNanos() {
        return tookTimeNanos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalLong(tookTimeNanos);
    }
}
