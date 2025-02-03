/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SnapshotIT extends OpenSearchRestHighLevelClientTestCase {

    private AcknowledgedResponse createTestRepository(String repository, String type, String settings) throws IOException {
        PutRepositoryRequest request = new PutRepositoryRequest(repository);
        request.settings(settings, MediaTypeRegistry.JSON);
        request.type(type);
        return execute(request, highLevelClient().snapshot()::createRepository, highLevelClient().snapshot()::createRepositoryAsync);
    }

    private CreateSnapshotResponse createTestSnapshot(CreateSnapshotRequest createSnapshotRequest) throws IOException {
        // assumes the repository already exists

        return execute(createSnapshotRequest, highLevelClient().snapshot()::create, highLevelClient().snapshot()::createAsync);
    }

    public void testCreateRepository() throws IOException {
        AcknowledgedResponse response = createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(response.isAcknowledged());
    }

    public void testSnapshotGetRepositoriesUsingParams() throws IOException {
        String testRepository = "test";
        assertTrue(createTestRepository(testRepository, FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());
        assertTrue(createTestRepository("other", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        GetRepositoriesRequest request = new GetRepositoriesRequest();
        request.repositories(new String[] { testRepository });
        GetRepositoriesResponse response = execute(
            request,
            highLevelClient().snapshot()::getRepository,
            highLevelClient().snapshot()::getRepositoryAsync
        );
        assertThat(1, equalTo(response.repositories().size()));
    }

    public void testSnapshotGetDefaultRepositories() throws IOException {
        assertTrue(createTestRepository("other", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());
        assertTrue(createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        GetRepositoriesResponse response = execute(
            new GetRepositoriesRequest(),
            highLevelClient().snapshot()::getRepository,
            highLevelClient().snapshot()::getRepositoryAsync
        );
        assertThat(2, equalTo(response.repositories().size()));
    }

    public void testSnapshotGetRepositoriesNonExistent() {
        String repository = "doesnotexist";
        GetRepositoriesRequest request = new GetRepositoriesRequest(new String[] { repository });
        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> execute(request, highLevelClient().snapshot()::getRepository, highLevelClient().snapshot()::getRepositoryAsync)
        );

        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(
            exception.getMessage(),
            equalTo("OpenSearch exception [type=repository_missing_exception, reason=[" + repository + "] missing]")
        );
    }

    public void testSnapshotDeleteRepository() throws IOException {
        String repository = "test";
        assertTrue(createTestRepository(repository, FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        GetRepositoriesRequest request = new GetRepositoriesRequest();
        GetRepositoriesResponse response = execute(
            request,
            highLevelClient().snapshot()::getRepository,
            highLevelClient().snapshot()::getRepositoryAsync
        );
        assertThat(1, equalTo(response.repositories().size()));

        DeleteRepositoryRequest deleteRequest = new DeleteRepositoryRequest(repository);
        AcknowledgedResponse deleteResponse = execute(
            deleteRequest,
            highLevelClient().snapshot()::deleteRepository,
            highLevelClient().snapshot()::deleteRepositoryAsync
        );

        assertTrue(deleteResponse.isAcknowledged());
    }

    public void testVerifyRepository() throws IOException {
        AcknowledgedResponse putRepositoryResponse = createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        VerifyRepositoryRequest request = new VerifyRepositoryRequest("test");
        VerifyRepositoryResponse response = execute(
            request,
            highLevelClient().snapshot()::verifyRepository,
            highLevelClient().snapshot()::verifyRepositoryAsync
        );
        assertThat(response.getNodes().size(), equalTo(1));
    }

    public void testCleanupRepository() throws IOException {
        AcknowledgedResponse putRepositoryResponse = createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        CleanupRepositoryRequest request = new CleanupRepositoryRequest("test");
        CleanupRepositoryResponse response = execute(
            request,
            highLevelClient().snapshot()::cleanupRepository,
            highLevelClient().snapshot()::cleanupRepositoryAsync
        );
        assertThat(response.result().bytes(), equalTo(0L));
        assertThat(response.result().blobs(), equalTo(0L));
    }

    public void testCreateSnapshot() throws Exception {
        String repository = "test_repository";
        assertTrue(createTestRepository(repository, FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        String snapshot = "test_snapshot";
        CreateSnapshotRequest request = new CreateSnapshotRequest(repository, snapshot);
        boolean waitForCompletion = randomBoolean();
        request.waitForCompletion(waitForCompletion);
        if (randomBoolean()) {
            request.userMetadata(randomUserMetadata());
        }
        request.partial(randomBoolean());
        request.includeGlobalState(randomBoolean());

        CreateSnapshotResponse response = createTestSnapshot(request);
        assertEquals(waitForCompletion ? RestStatus.OK : RestStatus.ACCEPTED, response.status());
        if (waitForCompletion == false) {
            // If we don't wait for the snapshot to complete we have to cancel it to not leak the snapshot task
            AcknowledgedResponse deleteResponse = execute(
                new DeleteSnapshotRequest(repository, snapshot),
                highLevelClient().snapshot()::delete,
                highLevelClient().snapshot()::deleteAsync
            );
            assertTrue(deleteResponse.isAcknowledged());
        }
    }

    public void testGetSnapshots() throws IOException {
        String repository = "test_repository";
        String snapshot1 = "test_snapshot1";
        String snapshot2 = "test_snapshot2";

        AcknowledgedResponse putRepositoryResponse = createTestRepository(repository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        CreateSnapshotRequest createSnapshotRequest1 = new CreateSnapshotRequest(repository, snapshot1);
        createSnapshotRequest1.waitForCompletion(true);
        CreateSnapshotResponse putSnapshotResponse1 = createTestSnapshot(createSnapshotRequest1);
        CreateSnapshotRequest createSnapshotRequest2 = new CreateSnapshotRequest(repository, snapshot2);
        createSnapshotRequest2.waitForCompletion(true);
        Map<String, Object> originalMetadata = randomUserMetadata();
        createSnapshotRequest2.userMetadata(originalMetadata);
        CreateSnapshotResponse putSnapshotResponse2 = createTestSnapshot(createSnapshotRequest2);
        // check that the request went ok without parsing JSON here. When using the high level client, check acknowledgement instead.
        assertEquals(RestStatus.OK, putSnapshotResponse1.status());
        assertEquals(RestStatus.OK, putSnapshotResponse2.status());

        GetSnapshotsRequest request;
        if (randomBoolean()) {
            request = new GetSnapshotsRequest(repository);
        } else if (randomBoolean()) {
            request = new GetSnapshotsRequest(repository, new String[] { "_all" });

        } else {
            request = new GetSnapshotsRequest(repository, new String[] { snapshot1, snapshot2 });
        }
        GetSnapshotsResponse response = execute(request, highLevelClient().snapshot()::get, highLevelClient().snapshot()::getAsync);

        assertEquals(2, response.getSnapshots().size());
        assertThat(
            response.getSnapshots().stream().map((s) -> s.snapshotId().getName()).collect(Collectors.toList()),
            contains("test_snapshot1", "test_snapshot2")
        );
        Optional<Map<String, Object>> returnedMetadata = response.getSnapshots()
            .stream()
            .filter(s -> s.snapshotId().getName().equals("test_snapshot2"))
            .findFirst()
            .map(SnapshotInfo::userMetadata);
        if (returnedMetadata.isPresent()) {
            assertEquals(originalMetadata, returnedMetadata.get());
        } else {
            assertNull("retrieved metadata is null, expected non-null metadata", originalMetadata);
        }
    }

    public void testSnapshotsStatus() throws IOException {
        String testRepository = "test";
        String testSnapshot = "snapshot";
        String testIndex = "test_index";

        AcknowledgedResponse putRepositoryResponse = createTestRepository(testRepository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        createIndex(testIndex, Settings.EMPTY);

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(testRepository, testSnapshot);
        createSnapshotRequest.indices(testIndex);
        createSnapshotRequest.waitForCompletion(true);
        CreateSnapshotResponse createSnapshotResponse = createTestSnapshot(createSnapshotRequest);
        // check that the request went ok without parsing JSON here. When using the high level client, check acknowledgement instead.
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        SnapshotsStatusRequest request = new SnapshotsStatusRequest();
        request.repository(testRepository);
        request.snapshots(new String[] { testSnapshot });
        SnapshotsStatusResponse response = execute(
            request,
            highLevelClient().snapshot()::status,
            highLevelClient().snapshot()::statusAsync
        );
        assertThat(response.getSnapshots().size(), equalTo(1));
        assertThat(response.getSnapshots().get(0).getSnapshot().getRepository(), equalTo(testRepository));
        assertThat(response.getSnapshots().get(0).getSnapshot().getSnapshotId().getName(), equalTo(testSnapshot));
        assertThat(response.getSnapshots().get(0).getIndices().containsKey(testIndex), is(true));
    }

    public void testRestoreSnapshot() throws IOException {
        String testRepository = "test";
        String testSnapshot = "snapshot_1";
        String testIndex = "test_index";
        String restoredIndex = testIndex + "_restored";

        AcknowledgedResponse putRepositoryResponse = createTestRepository(testRepository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        createIndex(testIndex, Settings.EMPTY);
        assertTrue("index [" + testIndex + "] should have been created", indexExists(testIndex));

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(testRepository, testSnapshot);
        createSnapshotRequest.indices(testIndex);
        createSnapshotRequest.waitForCompletion(true);
        if (randomBoolean()) {
            createSnapshotRequest.userMetadata(randomUserMetadata());
        }
        CreateSnapshotResponse createSnapshotResponse = createTestSnapshot(createSnapshotRequest);
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        deleteIndex(testIndex);
        assertFalse("index [" + testIndex + "] should have been deleted", indexExists(testIndex));

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(testRepository, testSnapshot);
        request.waitForCompletion(true);
        request.renamePattern(testIndex);
        request.renameReplacement(restoredIndex);

        RestoreSnapshotResponse response = execute(
            request,
            highLevelClient().snapshot()::restore,
            highLevelClient().snapshot()::restoreAsync
        );

        RestoreInfo restoreInfo = response.getRestoreInfo();
        assertThat(restoreInfo.name(), equalTo(testSnapshot));
        assertThat(restoreInfo.indices(), equalTo(Collections.singletonList(restoredIndex)));
        assertThat(restoreInfo.successfulShards(), greaterThan(0));
        assertThat(restoreInfo.failedShards(), equalTo(0));
    }

    public void testSnapshotHidden() throws IOException {
        String testRepository = "test";
        String testSnapshot = "snapshot_1";
        String testIndex = "test_index";

        AcknowledgedResponse putRepositoryResponse = createTestRepository(testRepository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        createIndex(
            testIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .build()
        );
        assertTrue("index [" + testIndex + "] should have been created", indexExists(testIndex));

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(testRepository, testSnapshot);
        createSnapshotRequest.indices("*");
        createSnapshotRequest.waitForCompletion(true);
        if (randomBoolean()) {
            createSnapshotRequest.userMetadata(randomUserMetadata());
        }
        CreateSnapshotResponse createSnapshotResponse = createTestSnapshot(createSnapshotRequest);
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        deleteIndex(testIndex);
        assertFalse("index [" + testIndex + "] should have been deleted", indexExists(testIndex));

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(testRepository, testSnapshot);
        request.waitForCompletion(true);
        request.indices(randomFrom(testIndex, "test_*"));
        request.renamePattern(testIndex);

        RestoreSnapshotResponse response = execute(
            request,
            highLevelClient().snapshot()::restore,
            highLevelClient().snapshot()::restoreAsync
        );

        RestoreInfo restoreInfo = response.getRestoreInfo();
        assertThat(restoreInfo.name(), equalTo(testSnapshot));
        assertThat(restoreInfo.indices(), equalTo(Collections.singletonList(testIndex)));
        assertThat(restoreInfo.successfulShards(), greaterThan(0));
        assertThat(restoreInfo.failedShards(), equalTo(0));
    }

    public void testDeleteSnapshot() throws IOException {
        String repository = "test_repository";
        String snapshot = "test_snapshot";

        AcknowledgedResponse putRepositoryResponse = createTestRepository(repository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repository, snapshot);
        createSnapshotRequest.waitForCompletion(true);
        if (randomBoolean()) {
            createSnapshotRequest.userMetadata(randomUserMetadata());
        }
        CreateSnapshotResponse createSnapshotResponse = createTestSnapshot(createSnapshotRequest);
        // check that the request went ok without parsing JSON here. When using the high level client, check acknowledgement instead.
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        DeleteSnapshotRequest request = new DeleteSnapshotRequest(repository, snapshot);
        AcknowledgedResponse response = execute(request, highLevelClient().snapshot()::delete, highLevelClient().snapshot()::deleteAsync);

        assertTrue(response.isAcknowledged());
    }

    public void testCloneSnapshot() throws IOException {
        String repository = "test_repository";
        String snapshot = "source_snapshot";
        String targetSnapshot = "target_snapshot";
        final String testIndex = "test_idx";

        createIndex(testIndex, Settings.EMPTY);
        assertTrue("index [" + testIndex + "] should have been created", indexExists(testIndex));

        AcknowledgedResponse putRepositoryResponse = createTestRepository(repository, FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(putRepositoryResponse.isAcknowledged());

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repository, snapshot);
        createSnapshotRequest.waitForCompletion(true);

        CreateSnapshotResponse createSnapshotResponse = createTestSnapshot(createSnapshotRequest);
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        CloneSnapshotRequest request = new CloneSnapshotRequest(repository, snapshot, targetSnapshot, new String[] { testIndex });
        AcknowledgedResponse response = execute(request, highLevelClient().snapshot()::clone, highLevelClient().snapshot()::cloneAsync);

        assertTrue(response.isAcknowledged());
    }

    private static Map<String, Object> randomUserMetadata() {
        if (randomBoolean()) {
            return null;
        }

        Map<String, Object> metadata = new HashMap<>();
        long fields = randomLongBetween(0, 4);
        for (int i = 0; i < fields; i++) {
            if (randomBoolean()) {
                metadata.put(
                    randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                    randomAlphaOfLengthBetween(5, 5)
                );
            } else {
                Map<String, Object> nested = new HashMap<>();
                long nestedFields = randomLongBetween(0, 4);
                for (int j = 0; j < nestedFields; j++) {
                    nested.put(
                        randomValueOtherThanMany(nested::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                        randomAlphaOfLengthBetween(5, 5)
                    );
                }
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)), nested);
            }
        }
        return metadata;
    }
}
