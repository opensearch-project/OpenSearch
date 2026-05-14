/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.plugin.stats.DataFusionNativeNodeStats;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Defines the {@code MemoryLayout.structLayout} mirroring the Rust buffer written by
 * {@code df_native_node_stats} and provides {@link VarHandle} accessors for each field.
 *
 * <p>The layout contains 4 × {@code JAVA_LONG} fields (32 bytes total):
 * <ul>
 *   <li>{@code native_search_task_current} — offset 0</li>
 *   <li>{@code native_search_task_total} — offset 8</li>
 *   <li>{@code native_search_shard_task_current} — offset 16</li>
 *   <li>{@code native_search_shard_task_total} — offset 24</li>
 * </ul>
 *
 * <p>This layout is independent of {@link StatsLayout} which serves plugin stats
 * via {@code df_stats()}.
 */
public final class NativeNodeStatsLayout {

    /** The struct layout mirroring the Rust {@code df_native_node_stats} output buffer. */
    public static final StructLayout LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("native_search_task_current"),
        ValueLayout.JAVA_LONG.withName("native_search_task_total"),
        ValueLayout.JAVA_LONG.withName("native_search_shard_task_current"),
        ValueLayout.JAVA_LONG.withName("native_search_shard_task_total")
    );

    static {
        assert LAYOUT.byteSize() == 32 : "NativeNodeStatsLayout size mismatch";
    }

    private static final VarHandle SEARCH_TASK_CURRENT = LAYOUT.varHandle(PathElement.groupElement("native_search_task_current"));
    private static final VarHandle SEARCH_TASK_TOTAL = LAYOUT.varHandle(PathElement.groupElement("native_search_task_total"));
    private static final VarHandle SHARD_TASK_CURRENT = LAYOUT.varHandle(PathElement.groupElement("native_search_shard_task_current"));
    private static final VarHandle SHARD_TASK_TOTAL = LAYOUT.varHandle(PathElement.groupElement("native_search_shard_task_total"));

    /**
     * Decode the 32-byte memory segment into a {@link DataFusionNativeNodeStats} instance.
     *
     * @param seg the memory segment containing the output of {@code df_native_node_stats}
     * @return a populated {@link DataFusionNativeNodeStats}
     */
    public static DataFusionNativeNodeStats readNativeNodeStats(MemorySegment seg) {
        return new DataFusionNativeNodeStats(
            (long) SEARCH_TASK_CURRENT.get(seg, 0L),
            (long) SEARCH_TASK_TOTAL.get(seg, 0L),
            (long) SHARD_TASK_CURRENT.get(seg, 0L),
            (long) SHARD_TASK_TOTAL.get(seg, 0L)
        );
    }

    private NativeNodeStatsLayout() {}
}
