/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.RemoteStoreSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Collecting stale segment metadata is the one half of remote store garbage collection a {@code segments_only} shard
 * still does, since its segments are remote and only its translog is not. The collector hangs off the refresh
 * listener, which is created from {@code isRemoteSegmentStoreNode()}; had it been gated on one of the predicates
 * this mode fails, the metadata would accumulate for the life of the index with nothing to bound it.
 */
public class SegmentsOnlyGarbageCollectionIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-gc-idx";
    private static final int METADATA_FILES_TO_KEEP = 1;
    private static final int INITIAL_COMMITS = 5;

    private int nextDocId = 0;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), METADATA_FILES_TO_KEEP)
            .build();
    }

    public void testStaleSegmentMetadataIsCollected() throws Exception {
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);
        indexAndCommit(INITIAL_COMMITS);

        Path metadataPath = segmentMetadataPath();
        assertThat(
            "the commits must have produced metadata to collect",
            listMetadataFiles(metadataPath).size(),
            greaterThan(METADATA_FILES_TO_KEEP)
        );

        // Collection runs after a commit, so the count is driven back down by committing rather than by waiting.
        assertBusy(() -> {
            indexAndCommit(1);
            assertThat(listMetadataFiles(metadataPath).size(), lessThanOrEqualTo(METADATA_FILES_TO_KEEP + 1));
        });
    }

    /** Only a commit carrying new documents produces a metadata file, so each iteration indexes before it flushes. */
    private void indexAndCommit(int commits) {
        for (int i = 0; i < commits; i++) {
            nextDocId = indexDocs(INDEX_NAME, nextDocId, 5);
            flush(INDEX_NAME);
        }
    }

    private Path segmentMetadataPath() {
        String prefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(getNodeSettings());
        return segmentRepoPath.resolve(
            getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, METADATA, prefix).buildAsString()
        );
    }

    private static List<String> listMetadataFiles(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            return files.map(file -> file.getFileName().toString()).collect(Collectors.toList());
        }
    }
}
