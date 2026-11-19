/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One payload of a streaming search response: an ordered list of memory segments, written back to back, and the
 * resource that owns their memory.
 * <p>
 * Direct segments are handed to the network layer without copying when the transport supports it, so the owner must
 * keep their memory valid and unmodified until {@link #release()} runs. Segment positions and limits are never modified.
 */
@ExperimentalApi
public final class StreamPayload {

    private final List<ByteBuffer> segments;
    private final long length;
    private final Releasable owner;
    private final AtomicBoolean released = new AtomicBoolean();

    /**
     * Creates a payload of {@code segments}, whose memory {@code owner} holds.
     *
     * @param segments the segments, in wire order
     * @param owner    released exactly once when the payload memory is no longer needed
     */
    public StreamPayload(List<ByteBuffer> segments, Releasable owner) {
        this.segments = List.copyOf(segments);
        this.owner = Objects.requireNonNull(owner, "owner");
        long total = 0;
        for (ByteBuffer segment : this.segments) {
            total += segment.remaining();
        }
        this.length = total;
    }

    /**
     * Returns the segments, in wire order; callers must not modify their position or limit.
     */
    public List<ByteBuffer> segments() {
        return segments;
    }

    /**
     * Returns the total number of bytes across all segments.
     */
    public long length() {
        return length;
    }

    /**
     * Releases the owner on the first call. Later calls do nothing.
     */
    public void release() {
        if (released.compareAndSet(false, true)) {
            owner.close();
        }
    }

    /**
     * Returns whether {@link #release()} has run.
     */
    public boolean isReleased() {
        return released.get();
    }
}
