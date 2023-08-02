/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.snapshots.blobstore;

import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;

public class RemoteStoreShardShallowCopySnapshotTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        String snapshot = "test-snapshot";
        long indexVersion = 1;
        long primaryTerm = 3;
        long commitGeneration = 5;
        long startTime = 123;
        long time = 123;
        int totalFileCount = 5;
        long totalSize = 5;
        String indexUUID = "syzhajds-ashdlfj";
        String remoteStoreRepository = "test-rs-repository";
        String repositoryBasePath = "test-repo-basepath";
        List<String> fileNames = new ArrayList<>(5);
        fileNames.addAll(Arrays.asList("file1", "file2", "file3", "file4", "file5"));
        RemoteStoreShardShallowCopySnapshot shardShallowCopySnapshot = new RemoteStoreShardShallowCopySnapshot(
            snapshot,
            indexVersion,
            primaryTerm,
            commitGeneration,
            startTime,
            time,
            totalFileCount,
            totalSize,
            indexUUID,
            remoteStoreRepository,
            repositoryBasePath,
            fileNames
        );
        String actual;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            shardShallowCopySnapshot.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            actual = Strings.toString(builder);
        }
        String expectedXContent = "{\"version\":\"1\",\"name\":\"test-snapshot\",\"index_version\":1,\"start_time\":123,\"time\":123,"
            + "\"number_of_files\":5,\"total_size\":5,\"index_uuid\":\"syzhajds-ashdlfj\",\"remote_store_repository\":"
            + "\"test-rs-repository\",\"commit_generation\":5,\"primary_term\":3,\"remote_store_repository_base_path\":"
            + "\"test-repo-basepath\",\"file_names\":[\"file1\",\"file2\",\"file3\",\"file4\",\"file5\"]}";
        assert Objects.equals(actual, expectedXContent) : "xContent is " + actual;
    }

    public void testFromXContent() throws IOException {
        String snapshot = "test-snapshot";
        long indexVersion = 1;
        long primaryTerm = 3;
        long commitGeneration = 5;
        long startTime = 123;
        long time = 123;
        int totalFileCount = 5;
        long totalSize = 5;
        String indexUUID = "syzhajds-ashdlfj";
        String remoteStoreRepository = "test-rs-repository";
        String repositoryBasePath = "test-repo-basepath";
        List<String> fileNames = new ArrayList<>(5);
        fileNames.addAll(Arrays.asList("file1", "file2", "file3", "file4", "file5"));
        RemoteStoreShardShallowCopySnapshot expectedShardShallowCopySnapshot = new RemoteStoreShardShallowCopySnapshot(
            snapshot,
            indexVersion,
            primaryTerm,
            commitGeneration,
            startTime,
            time,
            totalFileCount,
            totalSize,
            indexUUID,
            remoteStoreRepository,
            repositoryBasePath,
            fileNames
        );
        String xContent = "{\"version\":\"1\",\"name\":\"test-snapshot\",\"index_version\":1,\"start_time\":123,\"time\":123,"
            + "\"number_of_files\":5,\"total_size\":5,\"index_uuid\":\"syzhajds-ashdlfj\",\"remote_store_repository\":"
            + "\"test-rs-repository\",\"commit_generation\":5,\"primary_term\":3,\"remote_store_repository_base_path\":"
            + "\"test-repo-basepath\",\"file_names\":[\"file1\",\"file2\",\"file3\",\"file4\",\"file5\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            RemoteStoreShardShallowCopySnapshot actualShardShallowCopySnapshot = RemoteStoreShardShallowCopySnapshot.fromXContent(parser);
            assertEquals(actualShardShallowCopySnapshot.snapshot(), expectedShardShallowCopySnapshot.snapshot());
            assertEquals(
                actualShardShallowCopySnapshot.getRemoteStoreRepository(),
                expectedShardShallowCopySnapshot.getRemoteStoreRepository()
            );
            assertEquals(actualShardShallowCopySnapshot.getCommitGeneration(), expectedShardShallowCopySnapshot.getCommitGeneration());
            assertEquals(actualShardShallowCopySnapshot.getPrimaryTerm(), expectedShardShallowCopySnapshot.getPrimaryTerm());
            assertEquals(actualShardShallowCopySnapshot.startTime(), expectedShardShallowCopySnapshot.startTime());
            assertEquals(actualShardShallowCopySnapshot.time(), expectedShardShallowCopySnapshot.time());
            assertEquals(actualShardShallowCopySnapshot.totalSize(), expectedShardShallowCopySnapshot.totalSize());
            assertEquals(actualShardShallowCopySnapshot.totalFileCount(), expectedShardShallowCopySnapshot.totalFileCount());
        }
    }

    public void testFromXContentInvalid() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            String snapshot = "test-snapshot";
            long indexVersion = 1;
            long primaryTerm = 3;
            long commitGeneration = 5;
            long startTime = 123;
            long time = 123;
            int totalFileCount = 5;
            long totalSize = 5;
            String indexUUID = "syzhajds-ashdlfj";
            String remoteStoreRepository = "test-rs-repository";
            String repositoryBasePath = "test-repo-basepath";
            List<String> fileNames = new ArrayList<>(5);
            fileNames.addAll(Arrays.asList("file1", "file2", "file3", "file4", "file5"));
            String failure = null;
            String version = RemoteStoreShardShallowCopySnapshot.DEFAULT_VERSION;
            long length = Math.max(0, Math.abs(randomLong()));
            // random corruption
            switch (randomIntBetween(0, 8)) {
                case 0:
                    snapshot = null;
                    failure = "Invalid/Missing Snapshot Name";
                    break;
                case 1:
                    indexVersion = -Math.abs(randomLong());
                    failure = "Invalid Index Version";
                    break;
                case 2:
                    commitGeneration = -Math.abs(randomLong());
                    failure = "Invalid Commit Generation";
                    break;
                case 3:
                    primaryTerm = -Math.abs(randomLong());
                    failure = "Invalid Primary Term";
                    break;
                case 4:
                    indexUUID = null;
                    failure = "Invalid/Missing Index UUID";
                    break;
                case 5:
                    remoteStoreRepository = null;
                    failure = "Invalid/Missing Remote Store Repository";
                    break;
                case 6:
                    repositoryBasePath = null;
                    failure = "Invalid/Missing Repository Base Path";
                    break;
                case 7:
                    version = null;
                    failure = "Invalid Version Provided";
                    break;
                case 8:
                    break;
                default:
                    fail("shouldn't be here");
            }

            XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field(RemoteStoreShardShallowCopySnapshot.VERSION, version);
            builder.field(RemoteStoreShardShallowCopySnapshot.NAME, snapshot);
            builder.field(RemoteStoreShardShallowCopySnapshot.INDEX_VERSION, indexVersion);
            builder.field(RemoteStoreShardShallowCopySnapshot.START_TIME, startTime);
            builder.field(RemoteStoreShardShallowCopySnapshot.TIME, time);
            builder.field(RemoteStoreShardShallowCopySnapshot.TOTAL_FILE_COUNT, totalFileCount);
            builder.field(RemoteStoreShardShallowCopySnapshot.TOTAL_SIZE, totalSize);
            builder.field(RemoteStoreShardShallowCopySnapshot.INDEX_UUID, indexUUID);
            builder.field(RemoteStoreShardShallowCopySnapshot.REMOTE_STORE_REPOSITORY, remoteStoreRepository);
            builder.field(RemoteStoreShardShallowCopySnapshot.COMMIT_GENERATION, commitGeneration);
            builder.field(RemoteStoreShardShallowCopySnapshot.PRIMARY_TERM, primaryTerm);
            builder.field(RemoteStoreShardShallowCopySnapshot.REPOSITORY_BASE_PATH, repositoryBasePath);
            builder.startArray(RemoteStoreShardShallowCopySnapshot.FILE_NAMES);
            for (String fileName : fileNames) {
                builder.value(fileName);
            }
            builder.endArray();
            builder.endObject();
            byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));

            if (failure == null) {
                // No failures should read as usual
                final RemoteStoreShardShallowCopySnapshot remoteStoreShardShallowCopySnapshot;
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    remoteStoreShardShallowCopySnapshot = RemoteStoreShardShallowCopySnapshot.fromXContent(parser);
                }
                assertEquals(remoteStoreShardShallowCopySnapshot.snapshot(), snapshot);
                assertEquals(remoteStoreShardShallowCopySnapshot.getRemoteStoreRepository(), remoteStoreRepository);
                assertEquals(remoteStoreShardShallowCopySnapshot.getCommitGeneration(), commitGeneration);
                assertEquals(remoteStoreShardShallowCopySnapshot.getPrimaryTerm(), primaryTerm);
                assertEquals(remoteStoreShardShallowCopySnapshot.startTime(), startTime);
                assertEquals(remoteStoreShardShallowCopySnapshot.time(), time);
                assertEquals(remoteStoreShardShallowCopySnapshot.totalSize(), totalSize);
                assertEquals(remoteStoreShardShallowCopySnapshot.totalFileCount(), totalFileCount);
            } else {
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    RemoteStoreShardShallowCopySnapshot.fromXContent(parser);
                    fail("Should have failed with [" + failure + "]");
                } catch (IllegalArgumentException ex) {
                    assertThat(ex.getMessage(), containsString(failure));
                }
            }
        }
    }
}
