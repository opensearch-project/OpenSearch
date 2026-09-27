/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test verifying that snapshot listing tolerates legacy Elasticsearch version IDs in
 * snapshot metadata, and that restore of such snapshots fails gracefully. A real snapshot is taken
 * in an fs repository and its {@code snap-{uuid}.dat} blob is overwritten with one containing
 * {@code version_id: 7100299} (ES 7.10.2, which lacks the OpenSearch mask bit).
 */
public class LegacySnapshotVersionIT extends AbstractSnapshotIntegTestCase {

    private static final int LEGACY_ES_VERSION_ID = 7_10_02_99;

    private static final String SNAPSHOT_CODEC = "snapshot";
    private static final int SNAPSHOT_FORMAT_VERSION = 1;

    public void testListingSucceedsAndRestoreFailsForLegacyEsSnapshot() throws Exception {
        final Path repoPath = randomRepoPath();
        final String repoName = "legacy-test-repo";
        final String snapshotName = "legacy-es-snapshot";

        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repoPath)
                .put("compress", false)
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
        );

        createIndex("test-index");
        indexRandom(true, client().prepareIndex().setIndex("test-index").setSource("field", "value"));

        CreateSnapshotResponse createResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-index")
            .get();
        assertThat(createResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createResponse.getSnapshotInfo().successfulShards(), equalTo(createResponse.getSnapshotInfo().totalShards()));

        final String snapshotUUID = createResponse.getSnapshotInfo().snapshotId().getUUID();
        overwriteSnapshotBlobWithLegacyVersion(repoPath, snapshotName, snapshotUUID, LEGACY_ES_VERSION_ID);

        GetSnapshotsResponse getResponse = client().admin().cluster().prepareGetSnapshots(repoName).get();
        List<SnapshotInfo> snapshots = getResponse.getSnapshots();
        assertThat(snapshots.size(), equalTo(1));

        SnapshotInfo listedSnapshot = snapshots.get(0);
        assertThat(listedSnapshot.snapshotId().getName(), equalTo(snapshotName));
        assertThat(listedSnapshot.snapshotId().getUUID(), equalTo(snapshotUUID));
        assertThat(listedSnapshot.version(), nullValue());
        assertThat(listedSnapshot.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(listedSnapshot.indices(), equalTo(Collections.singletonList("test-index")));

        assertAcked(client().admin().indices().prepareDelete("test-index"));

        SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true).get()
        );
        assertThat(e.getMessage(), containsString("unknown or unsupported version"));
    }

    /**
     * Overwrites the {@code snap-{uuid}.dat} blob with a serialized snapshot info blob that uses the
     * given legacy {@code version_id}, matching the format produced by {@link BlobStoreRepository#SNAPSHOT_FORMAT}.
     */
    private void overwriteSnapshotBlobWithLegacyVersion(Path repoPath, String snapshotName, String snapshotUUID, int legacyVersionId)
        throws IOException {
        final String blobName = BlobStoreRepository.SNAPSHOT_PREFIX + snapshotUUID + ".dat";
        final Path blobPath = repoPath.resolve(blobName);

        ToXContent legacySnapshotContent = (builder, params) -> {
            builder.startObject("snapshot");
            builder.field("name", snapshotName);
            builder.field("uuid", snapshotUUID);
            builder.field("version_id", legacyVersionId);
            builder.startArray("indices");
            builder.value("test-index");
            builder.endArray();
            builder.startArray("data_streams");
            builder.endArray();
            builder.field("state", "SUCCESS");
            builder.field("include_global_state", true);
            builder.field("metadata", Collections.emptyMap());
            builder.field("start_time", 1_000_000L);
            builder.field("end_time", 2_000_000L);
            builder.field("total_shards", 1);
            builder.field("successful_shards", 1);
            builder.startArray("failures");
            builder.endArray();
            builder.endObject();
            return builder;
        };

        BytesReference bytes = serializeSnapshotBlob(legacySnapshotContent, blobName);
        Files.write(blobPath, BytesReference.toBytes(bytes), StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Serializes the given content into a valid {@code snap-*.dat} blob: Lucene codec header,
     * uncompressed SMILE x-content, and Lucene codec footer.
     */
    private BytesReference serializeSnapshotBlob(ToXContent content, String blobName) throws IOException {
        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "LegacySnapshotVersionIT.serializeSnapshotBlob(blob=\"" + blobName + "\")",
                    blobName,
                    outputStream,
                    4096
                )
            ) {
                CodecUtil.writeHeader(indexOutput, SNAPSHOT_CODEC, SNAPSHOT_FORMAT_VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() {}
                };
                    XContentBuilder builder = MediaTypeRegistry.contentBuilder(
                        XContentType.SMILE,
                        CompressorRegistry.none().threadLocalOutputStream(indexOutputOutputStream)
                    )
                ) {
                    builder.startObject();
                    content.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                }
                CodecUtil.writeFooter(indexOutput);
            }
            return outputStream.bytes();
        }
    }
}
