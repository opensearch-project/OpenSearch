/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class PmeDataKeyCacheTests extends OpenSearchTestCase {

    private static final int KEY_LEN = PmeKeyDerivation.DATA_KEY_BYTES;
    private static final String INDEX_UUID = "test-index-uuid";
    private static final String DATA_KEY_ID = PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID;
    private static final int SHARD_A = 0;
    private static final int SHARD_B = 1;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PmeDataKeyCache.initialize();
    }

    @Override
    public void tearDown() throws Exception {
        PmeDataKeyCache.reset();
        super.tearDown();
    }

    // ---- getOrLoad ----

    public void testGetOrLoadCallsLoaderOnCacheMiss() throws IOException {
        byte[] rawKey = new byte[KEY_LEN];
        Arrays.fill(rawKey, (byte) 7);

        PmeDataKey result = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> rawKey.clone());

        byte[] got = result.bytes();
        try {
            assertArrayEquals(rawKey, got);
        } finally {
            Arrays.fill(got, (byte) 0);
        }
    }

    public void testGetOrLoadReturnsCachedValueOnSecondCall() throws IOException {
        AtomicInteger loaderCalls = new AtomicInteger(0);
        byte[] rawKey = new byte[KEY_LEN];

        PmeDataKey first = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> {
            loaderCalls.incrementAndGet();
            return rawKey.clone();
        });

        PmeDataKey second = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> {
            loaderCalls.incrementAndGet();
            return rawKey.clone();
        });

        assertEquals("loader must be called exactly once", 1, loaderCalls.get());
        assertSame("must return the identical cached instance", first, second);
    }

    public void testGetOrLoadZerosRawKeyAfterConstruction() throws IOException {
        byte[] sentinel = new byte[KEY_LEN];
        Arrays.fill(sentinel, (byte) 0x55);
        byte[] copy = sentinel.clone();

        PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> sentinel);

        byte[] zeros = new byte[KEY_LEN];
        assertArrayEquals("loader array must be zeroed after caching", zeros, sentinel);

        PmeDataKey cached = PmeDataKeyCache.getInstance().getOrLoad(
            INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> { throw new IOException("should not be called"); }
        );
        byte[] got = cached.bytes();
        try {
            assertArrayEquals(copy, got);
        } finally {
            Arrays.fill(got, (byte) 0);
        }
    }

    // ---- evict ----

    public void testEvictZerosKeyMaterialAndForcesReload() throws IOException {
        byte[] rawKey = new byte[KEY_LEN];
        Arrays.fill(rawKey, (byte) 42);

        PmeDataKey inserted = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> rawKey.clone());
        PmeDataKeyCache.getInstance().evict(INDEX_UUID, SHARD_A, DATA_KEY_ID);

        byte[] afterEviction = inserted.bytes();
        try {
            assertArrayEquals("evicted key must be zeroed", new byte[KEY_LEN], afterEviction);
        } finally {
            Arrays.fill(afterEviction, (byte) 0);
        }

        AtomicInteger reloadCount = new AtomicInteger(0);
        PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> {
            reloadCount.incrementAndGet();
            return rawKey.clone();
        });
        assertEquals("loader must be called again after eviction", 1, reloadCount.get());
    }

    public void testEvictNonExistentKeyIsNoOp() {
        PmeDataKeyCache.getInstance().evict(INDEX_UUID, SHARD_A, DATA_KEY_ID);
    }

    // ---- shard isolation ----

    public void testDifferentShardsHaveIndependentEntries() throws IOException {
        byte[] keyA = new byte[KEY_LEN];
        Arrays.fill(keyA, (byte) 1);
        byte[] keyB = new byte[KEY_LEN];
        Arrays.fill(keyB, (byte) 2);

        PmeDataKey a = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_A, DATA_KEY_ID, () -> keyA.clone());
        PmeDataKey b = PmeDataKeyCache.getInstance().getOrLoad(INDEX_UUID, SHARD_B, DATA_KEY_ID, () -> keyB.clone());

        assertNotSame(a, b);

        byte[] gotA = a.bytes();
        byte[] gotB = b.bytes();
        try {
            assertArrayEquals(keyA, gotA);
            assertArrayEquals(keyB, gotB);
        } finally {
            Arrays.fill(gotA, (byte) 0);
            Arrays.fill(gotB, (byte) 0);
        }

        PmeDataKeyCache.getInstance().evict(INDEX_UUID, SHARD_A, DATA_KEY_ID);
        byte[] bAfter = b.bytes();
        try {
            assertArrayEquals("evicting shard A must not zero shard B", keyB, bAfter);
        } finally {
            Arrays.fill(bAfter, (byte) 0);
        }
    }
}

