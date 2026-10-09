/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Map;

/**
 * Unit tests for {@link ParquetSegmentBindings}: register/resolve, per-snapshot release with the
 * newest-first retention rule, and whole-shard removal.
 */
public class ParquetSegmentBindingsTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("idx", "uuid"), 0);

    private static ParquetSegmentBindings.Binding binding(String path) {
        return new ParquetSegmentBindings.Binding(Path.of(path), 0L);
    }

    /** A registered generation resolves to its binding; an unknown generation or shard resolves to null. */
    public void testRegisterAndResolve() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of(5L, binding("/data/parquet/seg_5.parquet")));

        assertEquals(Path.of("/data/parquet/seg_5.parquet"), bindings.resolve(SHARD, 5L).parquetFile());
        assertNull("unknown generation resolves to null", bindings.resolve(SHARD, 6L));
        assertNull("unknown shard resolves to null", bindings.resolve(new ShardId(new Index("other", "u2"), 0), 5L));
    }

    /** When two live snapshots bind the same generation, resolve returns the newest snapshot's binding. */
    public void testResolveSearchesNewestSnapshotFirst() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of(5L, binding("/data/parquet/seg_5_old.parquet")));
        bindings.register(SHARD, 2L, Map.of(5L, binding("/data/parquet/seg_5_new.parquet")));

        assertEquals(Path.of("/data/parquet/seg_5_new.parquet"), bindings.resolve(SHARD, 5L).parquetFile());
    }

    /** Releasing a snapshot keeps a shared generation resolvable through the remaining live snapshot. */
    public void testReleaseKeepsGenerationLiveViaOtherSnapshot() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of(5L, binding("/data/parquet/seg_5_old.parquet")));
        bindings.register(SHARD, 2L, Map.of(5L, binding("/data/parquet/seg_5_new.parquet")));

        bindings.release(SHARD, 2L);
        assertNotNull("older live snapshot keeps generation 5 resolvable", bindings.resolve(SHARD, 5L));
        assertEquals(Path.of("/data/parquet/seg_5_old.parquet"), bindings.resolve(SHARD, 5L).parquetFile());

        bindings.release(SHARD, 1L);
        assertNull("releasing the last snapshot drops the generation", bindings.resolve(SHARD, 5L));
    }

    /** Releasing an unknown snapshot id is a no-op and never throws. */
    public void testReleaseUnknownSnapshotIsNoOp() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of(5L, binding("/data/parquet/seg_5.parquet")));

        bindings.release(SHARD, 99L);
        assertNotNull(bindings.resolve(SHARD, 5L));
        bindings.release(new ShardId(new Index("other", "u2"), 0), 1L); // unknown shard: no throw
    }

    /** removeShard drops every snapshot's bindings for that shard at once. */
    public void testRemoveShardDropsAllBindings() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of(5L, binding("/data/parquet/seg_5.parquet")));
        bindings.register(SHARD, 2L, Map.of(6L, binding("/data/parquet/seg_6.parquet")));

        bindings.removeShard(SHARD);
        assertNull(bindings.resolve(SHARD, 5L));
        assertNull(bindings.resolve(SHARD, 6L));
    }

    /** An empty generation map is tracked (so the snapshot can be released) but resolves nothing. */
    public void testRegisterEmptyMapResolvesNothing() {
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        bindings.register(SHARD, 1L, Map.of());
        assertNull(bindings.resolve(SHARD, 5L));
        bindings.release(SHARD, 1L); // no throw
    }
}
