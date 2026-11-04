/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.index.translog.Translog;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * Encapsulates an Index Version in the translog
 *
 * @opensearch.internal
 */
final class IndexVersionValue extends VersionValue {

    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IndexVersionValue.class);
    private static final long TRANSLOG_LOC_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Translog.Location.class);

    /**
     * Exactly one of these is non-null. When the version value is produced inline (the normal path and every
     * non-batched engine) {@link #translogLocation} is set directly. When it is produced by a batched translog
     * append whose actual {@link Translog.Location} is not yet known, {@link #pending} holds the deferred location
     * and {@link #getLocation()} resolves it by forcing the containing batch chunk to append synchronously.
     */
    private final Translog.Location translogLocation;
    private final PendingLocation pending;

    IndexVersionValue(Translog.Location translogLocation, long version, long seqNo, long term) {
        super(version, seqNo, term);
        this.translogLocation = translogLocation;
        this.pending = null;
    }

    private IndexVersionValue(PendingLocation pending, long version, long seqNo, long term) {
        super(version, seqNo, term);
        assert pending != null : "pending location must not be null";
        this.translogLocation = null;
        this.pending = pending;
    }

    /**
     * Create a version value whose translog {@link Translog.Location} is not yet known because its operation was
     * appended to a not-yet-flushed translog batch. {@link #getLocation()} resolves it via {@code pending}.
     */
    static IndexVersionValue withPendingLocation(PendingLocation pending, long version, long seqNo, long term) {
        return new IndexVersionValue(pending, version, seqNo, term);
    }

    @Override
    public long ramBytesUsed() {
        // A pending location resolves to exactly one Translog.Location, so account for it the same way.
        return RAM_BYTES_USED + (translogLocation == null && pending == null ? 0L : TRANSLOG_LOC_RAM_BYTES_USED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IndexVersionValue that = (IndexVersionValue) o;
        return Objects.equals(translogLocation, that.translogLocation) && Objects.equals(pending, that.pending);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), translogLocation, pending);
    }

    @Override
    public String toString() {
        return "IndexVersionValue{"
            + "version="
            + version
            + ", seqNo="
            + seqNo
            + ", term="
            + term
            + ", location="
            + (pending == null ? translogLocation : pending)
            + '}';
    }

    @Override
    public Translog.Location getLocation() {
        if (pending != null) {
            return pending.resolve();
        }
        return translogLocation;
    }

    /**
     * A translog {@link Translog.Location} awaiting a batched append. Any reader resolving the location forces the
     * current batch chunk to append synchronously; concurrent readers join the same serialized flush and observe the
     * same completion or failure.
     *
     * @opensearch.internal
     */
    static final class PendingLocation {
        /** Flushes the batch chunk containing this pending location. */
        interface Flusher {
            void flushForPendingRead();
        }

        private final Flusher flusher;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Translog.Location location;
        private volatile RuntimeException failure;

        PendingLocation(Flusher flusher) {
            this.flusher = flusher;
        }

        void complete(Translog.Location resolved) {
            this.location = resolved;
            latch.countDown();
        }

        void completeExceptionally(RuntimeException ex) {
            this.failure = ex;
            latch.countDown();
        }

        Translog.Location resolve() {
            if (latch.getCount() != 0) {
                // The batch serializes concurrent flushes. This call either appends the chunk containing this entry
                // or joins the flush already in progress; it does not wait for the owning bulk to finish.
                flusher.flushForPendingRead();
            }
            if (latch.getCount() != 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for a pending translog location", e);
                }
            }
            return result();
        }

        private Translog.Location result() {
            final RuntimeException ex = failure;
            if (ex != null) {
                throw ex;
            }
            return location;
        }

        @Override
        public String toString() {
            return "pending[" + (latch.getCount() == 0 ? (failure != null ? "failed" : location) : "unresolved") + "]";
        }
    }
}
