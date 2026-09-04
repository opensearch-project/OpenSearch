/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryStats;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class CatalogRepositoryTests extends OpenSearchTestCase {

    private static CatalogRepository newRepo(RepositoryMetadata metadata) {
        return new CatalogRepository(metadata) {};
    }

    private static RepositoryMetadata testMetadata() {
        return new RepositoryMetadata("test-catalog", "iceberg_s3tables", Settings.EMPTY);
    }

    public void testConstructorRejectsNullMetadata() {
        expectThrows(NullPointerException.class, () -> newRepo(null));
    }

    public void testGetMetadataReturnsConstructorValue() {
        RepositoryMetadata metadata = testMetadata();
        CatalogRepository repo = newRepo(metadata);
        assertSame(metadata, repo.getMetadata());
    }

    public void testIsSystemRepositoryReturnsTrue() {
        assertTrue(newRepo(testMetadata()).isSystemRepository());
    }

    public void testIsReadOnlyReturnsFalse() {
        assertFalse(newRepo(testMetadata()).isReadOnly());
    }

    public void testGetRepositoryDataReturnsEmpty() {
        CatalogRepository repo = newRepo(testMetadata());
        AtomicReference<RepositoryData> captured = new AtomicReference<>();
        repo.getRepositoryData(ActionListener.wrap(captured::set, e -> fail("unexpected failure: " + e)));
        assertSame(RepositoryData.EMPTY, captured.get());
    }

    public void testStatsReturnsEmpty() {
        assertSame(RepositoryStats.EMPTY_STATS, newRepo(testMetadata()).stats());
    }

    public void testThrottleTimesAreZero() {
        CatalogRepository repo = newRepo(testMetadata());
        assertEquals(0L, repo.getSnapshotThrottleTimeInNanos());
        assertEquals(0L, repo.getRestoreThrottleTimeInNanos());
        assertEquals(0L, repo.getRemoteUploadThrottleTimeInNanos());
        assertEquals(0L, repo.getRemoteDownloadThrottleTimeInNanos());
        assertEquals(0L, repo.getLowPriorityRemoteDownloadThrottleTimeInNanos());
    }

    public void testVerificationHooksAreNoOps() {
        CatalogRepository repo = newRepo(testMetadata());
        assertNull(repo.startVerification());
        repo.endVerification("unused-token");
        // verify() is also a no-op; passing null DiscoveryNode is acceptable because the
        // default implementation never reads it.
        repo.verify("unused-token", null);
    }

    public void testUpdateStateIsNoOp() {
        // Should not throw when called with any cluster state (or null).
        newRepo(testMetadata()).updateState((ClusterState) null);
    }

    public void testSnapshotOperationsThrowUnsupported() {
        CatalogRepository repo = newRepo(testMetadata());
        SnapshotId snapshotId = new SnapshotId("snap", "snap-uuid");
        expectThrows(UnsupportedOperationException.class, () -> repo.getSnapshotInfo(snapshotId));
        expectThrows(UnsupportedOperationException.class, () -> repo.getSnapshotGlobalMetadata(snapshotId));
        expectThrows(UnsupportedOperationException.class, () -> repo.getShardSnapshotStatus(snapshotId, null, null));
    }
}
