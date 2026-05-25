/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link ParquetDataFormatStoreHandler}.
 *
 * <p>These tests cover the constructor's validation of {@code remotePtr} on warm nodes.
 * The tests deliberately do NOT reach the native FFM call into Rust — they verify that
 * the {@code IllegalStateException} is thrown early, before any Rust code is invoked.
 *
 * <p>{@code NativeStoreRepository} is a Java {@code record} (implicitly final) and cannot
 * be mocked with standard Mockito. Tests use {@code NativeStoreRepository.EMPTY} and
 * {@code null} to exercise the dead-repo and null-repo paths.
 */
public class ParquetDataFormatStoreHandlerTests extends OpenSearchTestCase {

    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);

    /**
     * Test: hot node with null repo → handler created as no-op (NativeStoreHandle.EMPTY).
     *
     * On hot nodes, {@code remotePtr} validation is skipped entirely — the handler
     * just stores {@code NativeStoreHandle.EMPTY} and all operations become no-ops.
     * No exception should be thrown regardless of the repo state.
     */
    public void testHotNode_NullRepo_CreatesEmptyHandler() {
        // isWarm=false → skips all validation, creates NativeStoreHandle.EMPTY
        ParquetDataFormatStoreHandler handler = new ParquetDataFormatStoreHandler(TEST_SHARD_ID, false, null, null);
        assertFalse("Hot handler with null repo should have a dead (empty) store handle", handler.getFormatStoreHandle().isLive());
    }

    /**
     * Test: hot node with NativeStoreRepository.EMPTY repo → handler created as no-op.
     *
     * Even if a non-null (but empty/dead) repo is passed to a hot node handler, it is ignored.
     * Hot nodes do not create a native TieredObjectStore regardless of the repo state.
     */
    public void testHotNode_DeadRepo_CreatesEmptyHandler() throws Exception {
        // NativeStoreRepository.EMPTY wraps NativeStoreHandle.EMPTY → isLive()=false
        ParquetDataFormatStoreHandler handler = new ParquetDataFormatStoreHandler(
            TEST_SHARD_ID,
            false,
            NativeStoreRepository.EMPTY,
            null
        );
        assertFalse("Hot handler should always create an empty (no-op) store handle", handler.getFormatStoreHandle().isLive());
        handler.close();
    }

    /**
     * Test: warm node with null repo → IllegalStateException thrown.
     *
     * When {@code repo == null}, the constructor evaluates:
     * {@code remotePtr = (repo != null && repo.isLive()) ? repo.getPointer() : 0L}
     * → remotePtr = 0L.
     *
     * Passing a null pointer to the Rust {@code TieredObjectStore} would cause undefined
     * behavior (segfault or memory corruption). The constructor must reject this eagerly
     * with a clear error message before any native code is invoked.
     */
    public void testWarmNode_NullRepo_ThrowsIllegalStateException() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new ParquetDataFormatStoreHandler(TEST_SHARD_ID, true, null, null)
        );
        assertThat(
            "Error message should identify remotePtr=0 as the cause",
            e.getMessage(),
            containsString("remotePtr=0")
        );
        assertThat("Error message should include the shard ID for debugging", e.getMessage(), containsString("test-index"));
    }

    /**
     * Test: warm node with {@code NativeStoreRepository.EMPTY} (dead repo) → IllegalStateException thrown.
     *
     * {@code NativeStoreRepository.EMPTY} wraps {@code NativeStoreHandle.EMPTY}, which has
     * {@code isLive()==false}. The constructor evaluates:
     * {@code remotePtr = (repo != null && repo.isLive()) ? repo.getPointer() : 0L}
     * → {@code repo.isLive()==false} → remotePtr = 0L → assertion fires.
     *
     * This is the real-world failure mode: a warm shard is opened before the
     * NativeStoreRepository has been properly initialized.
     */
    public void testWarmNode_DeadRepo_ThrowsIllegalStateException() {
        // NativeStoreRepository.EMPTY has isLive()=false, so remotePtr evaluates to 0L
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new ParquetDataFormatStoreHandler(TEST_SHARD_ID, true, NativeStoreRepository.EMPTY, null)
        );
        assertThat(
            "Error message should identify that remotePtr=0 is the cause",
            e.getMessage(),
            containsString("remotePtr=0")
        );
    }

    /**
     * Test: seed() is a no-op on hot nodes (storeHandle is EMPTY).
     *
     * Verifies that calling seed() on a handler created for a hot node does not
     * throw any exception and silently ignores the files map.
     */
    public void testSeed_HotNode_IsNoOp() throws Exception {
        ParquetDataFormatStoreHandler handler = new ParquetDataFormatStoreHandler(TEST_SHARD_ID, false, null, null);
        handler.seed(java.util.Collections.emptyMap());  // should not throw
        handler.close();
    }

    /**
     * Test: onUploaded() and onRemoved() are no-ops on hot nodes.
     */
    public void testOnUploadedAndOnRemoved_HotNode_AreNoOps() throws Exception {
        ParquetDataFormatStoreHandler handler = new ParquetDataFormatStoreHandler(TEST_SHARD_ID, false, null, null);
        handler.onUploaded("test.parquet", "remote/path/test.parquet", 1024L);  // should not throw
        handler.onRemoved("test.parquet");  // should not throw
        handler.close();
    }

    /**
     * Test: getFormatStoreHandle() returns a non-null empty (dead) handle on hot nodes.
     *
     * The method must never return null, even on hot nodes where no native store is created.
     */
    public void testGetFormatStoreHandle_HotNode_ReturnsNonNullEmptyHandle() throws Exception {
        ParquetDataFormatStoreHandler handler = new ParquetDataFormatStoreHandler(TEST_SHARD_ID, false, null, null);
        NativeStoreHandle handle = handler.getFormatStoreHandle();
        assertNotNull("getFormatStoreHandle() must never return null", handle);
        assertFalse("Hot node handler should return a non-live (empty) handle", handle.isLive());
        handler.close();
    }
}
