/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.OpenSearchTestCase.getShardLevelBlobPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Shared helpers for DFA integration tests.
 *
 * <p>Two assertions serve different purposes:
 * <ul>
 *   <li>{@link #assertCatalogMatchesLocalAndRemote}: replication-focused. The catalog snapshot
 *       must be a subset of both the local store directory and the remote directory listing.
 *       Cheap; does not touch the blob store on disk.
 *   <li>{@link #assertCatalogMatchesUploadedBlobs}: upload-focused. Each catalog file must be
 *       present in the upload map AND physically on disk under the shard's remote blob path,
 *       validating end-to-end upload correctness.
 * </ul>
 */
final class DataFormatAwareITUtils {

    private DataFormatAwareITUtils() {}

    /**
     * Replication invariant: every file in the catalog must exist locally on the shard and be
     * listed by the remote segment directory. Extra Lucene infra files (write.lock, segments_N)
     * may exist locally without appearing in the catalog; that's expected.
     *
     * <p>The {@code segments_N} file is excluded from the catalog comparison because its
     * generation is managed independently on primary vs replica (matching
     * {@link org.opensearch.index.engine.NRTReplicationEngine}): the replica writes its own local
     * {@code segments_N} while the catalog tracks the primary's file from the last replication.
     * These can legitimately differ between the catalog, local disk, and remote.
     */
    static void assertCatalogMatchesLocalAndRemote(IndexShard shard) throws IOException {
        Set<String> catalog = catalogFilesExcludingSegments(shard);
        Set<String> local = localFiles(shard);
        Set<String> remote = new HashSet<>(Arrays.asList(shard.getRemoteDirectory().listAll()));

        assertFalse("catalog snapshot has no files on " + shard.routingEntry(), catalog.isEmpty());
        assertSubset("local store directory", catalog, local, shard);
        assertSubset("remote directory listing", catalog, remote, shard);
    }

    /**
     * Upload invariant: each catalog file has a corresponding upload-map entry AND the uploaded
     * blob physically exists under the shard's remote blob path. Routes non-Lucene files through
     * their format subpath (e.g. {@code parquet/}).
     *
     * <p>The {@code segments_N} file is excluded because its generation is managed independently
     * on primary vs replica; see {@link #assertCatalogMatchesLocalAndRemote}.
     */
    static void assertCatalogMatchesUploadedBlobs(IndexShard shard, Client client, Settings nodeSettings, Path segmentRepoPath)
        throws IOException {
        Set<String> catalog = catalogFilesExcludingSegments(shard);
        Map<String, UploadedSegmentMetadata> uploadMap = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
        assertFalse("catalog snapshot has no files on " + shard.routingEntry(), catalog.isEmpty());

        Path shardDiskPath = segmentRepoPath.resolve(shardBlobPath(shard, client, nodeSettings).buildAsString());
        for (String originalName : catalog) {
            UploadedSegmentMetadata md = uploadMap.get(originalName);
            assertFalse(
                "no upload-map entry for " + originalName + " on " + shard.routingEntry() + "; keys=" + uploadMap.keySet(),
                md == null
            );
            int sep = originalName.indexOf('/');
            Path blobDir = sep < 0 ? shardDiskPath : shardDiskPath.resolve(originalName.substring(0, sep));
            Path blobFile = blobDir.resolve(md.getUploadedFilename());
            assertTrue(
                "remote blob missing on disk: " + blobFile + " for catalog file " + originalName + " on " + shard.routingEntry(),
                Files.isRegularFile(blobFile)
            );
        }
    }

    /**
     * Cross-shard invariant: every data file the primary uploaded to remote must exist on the
     * replica's local disk. Validates that segment replication transferred the data files from
     * the primary's remote-store uploads to the replica.
     *
     * <p>The {@code segments_N} file is excluded because its generation is managed independently
     * on primary vs replica; see {@link #assertCatalogMatchesLocalAndRemote}.
     *
     * <p>File layout on disk:
     * <ul>
     *   <li>Lucene files live under {@code <shard>/index/}</li>
     *   <li>Non-default format files (e.g. parquet) live under {@code <shard>/<format>/}</li>
     * </ul>
     */
    static void assertPrimaryUploadMapOnReplicaDisk(IndexShard primary, IndexShard replica) {
        Map<String, UploadedSegmentMetadata> uploadMap = primary.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
        assertFalse("primary upload map is empty on " + primary.routingEntry(), uploadMap.isEmpty());

        for (String originalName : uploadMap.keySet()) {
            if (originalName.startsWith("segments_")) {
                continue; // segments_N diverges between primary and replica by design
            }
            final Path onDisk;
            int sep = originalName.indexOf('/');
            if (sep < 0) {
                // default (lucene) format — lives under <shard>/index/
                onDisk = replica.shardPath().resolveIndex().resolve(originalName);
            } else {
                // non-default format (e.g. parquet/...) — lives under <shard>/<format>/
                String format = originalName.substring(0, sep);
                String file = originalName.substring(sep + 1);
                onDisk = replica.shardPath().getDataPath().resolve(format).resolve(file);
            }
            assertTrue(
                "primary-uploaded file missing on replica disk: " + onDisk + " (originalName=" + originalName + ")",
                Files.isRegularFile(onDisk)
            );
        }
    }

    static Set<String> catalogFiles(IndexShard shard) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = shard.getCatalogSnapshot()) {
            return new HashSet<>(ref.get().getFiles(true));
        }
    }

    /**
     * Returns catalog files excluding the Lucene {@code segments_N} file. Useful when comparing
     * primary vs replica catalogs: replicas write their own {@code segments_N} with an independent
     * generation (matching {@link org.opensearch.index.engine.NRTReplicationEngine}), so the
     * {@code segments_N} filename legitimately differs across shard copies.
     */
    static Set<String> catalogFilesExcludingSegments(IndexShard shard) throws IOException {
        Set<String> files = catalogFiles(shard);
        files.removeIf(f -> f.startsWith("segments_"));
        return files;
    }

    static Set<String> localFiles(IndexShard shard) throws IOException {
        return new HashSet<>(Arrays.asList(shard.store().directory().listAll()));
    }

    private static void assertSubset(String rhsName, Set<String> catalog, Set<String> rhs, IndexShard shard) {
        Set<String> missing = new HashSet<>(catalog);
        missing.removeAll(rhs);
        assertTrue(
            "catalog files missing from " + rhsName + " on " + shard.routingEntry() + ": " + missing + "; " + rhsName + "=" + rhs,
            missing.isEmpty()
        );
    }

    private static BlobPath shardBlobPath(IndexShard shard, Client client, Settings nodeSettings) {
        String prefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(nodeSettings);
        return getShardLevelBlobPath(
            client,
            shard.shardId().getIndexName(),
            new BlobPath(),
            String.valueOf(shard.shardId().id()),
            DataCategory.SEGMENTS,
            DataType.DATA,
            prefix
        );
    }
}
