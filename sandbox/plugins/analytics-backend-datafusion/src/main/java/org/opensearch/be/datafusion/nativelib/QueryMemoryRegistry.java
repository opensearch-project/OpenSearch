/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.NativeCall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Java-side view of the Rust {@code QUERY_REGISTRY}, exposing per-query memory
 * tracking metrics (current + peak bytes, wall time, completion status).
 *
 * <h2>Two-phase snapshot</h2>
 * <ol>
 *   <li>Ask the native side how many entries the registry holds via
 *       {@link NativeBridge#queryRegistryLen()}.</li>
 *   <li>Allocate a buffer sized for that count and hand it to
 *       {@link NativeBridge#queryRegistrySnapshot(java.lang.foreign.MemorySegment, int)},
 *       which writes entries back-to-back in the {@link QueryRegistryLayout}
 *       format and returns the actual number written.</li>
 * </ol>
 *
 * <p>Between phase 1 and phase 2 the registry can grow — new queries register
 * or complete. The native snapshot truncates silently when there are more
 * entries than the buffer holds, and Java exposes the actual written count, so
 * callers never see a partially-populated entry. If the registry is smaller
 * than the buffer at snapshot time, only the written prefix is decoded and
 * the remainder is ignored.
 */
public final class QueryMemoryRegistry {

    private static final Logger logger = LogManager.getLogger(QueryMemoryRegistry.class);

    private QueryMemoryRegistry() {}

    /**
     * Snapshot the current per-query memory registry.
     *
     * <p>Returns an unmodifiable {@link List} of {@link QueryMemoryUsage}
     * records — one per live or completed-but-undrained query. Ordering is
     * unspecified (mirrors DashMap iteration order on the Rust side).
     *
     * @return zero-or-more entries; never {@code null}.
     */
    public static List<QueryMemoryUsage> snapshot() {
        int len = NativeBridge.queryRegistryLen();
        logger.info("[nativemem-bp] ffm.snapshot: phase-1 queryRegistryLen={}", len);
        if (len <= 0) {
            return Collections.emptyList();
        }
        try (var call = new NativeCall()) {
            // buf() returns an 8-byte aligned segment (Arena.allocate honours the
            // alignment of the native layout we stride through in QueryRegistryLayout).
            long bytes = (long) len * QueryRegistryLayout.ENTRY_BYTES;
            if (bytes > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native registry too large for a single snapshot: " + len + " entries");
            }
            var seg = call.buf((int) bytes);
            int written = NativeBridge.queryRegistrySnapshot(seg, len);
            logger.info(
                "[nativemem-bp] ffm.snapshot: phase-2 queryRegistrySnapshot wrote={} entries into buffer of capacity={}",
                written,
                len
            );
            List<QueryMemoryUsage> out = new ArrayList<>(written);
            for (int i = 0; i < written; i++) {
                out.add(QueryRegistryLayout.readEntry(seg, i));
            }
            return Collections.unmodifiableList(out);
        }
    }
}
