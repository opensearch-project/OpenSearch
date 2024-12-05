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

package org.opensearch.snapshots.mockstore;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class MockEventuallyConsistentRepositoryTests extends OpenSearchTestCase {

    private final RecoverySettings recoverySettings = new RecoverySettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    public void testReadAfterWriteConsistently() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
                xContentRegistry(),
                BlobStoreTestUtil.mockClusterService(),
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            try (InputStream in = blobContainer.readBlob(blobName)) {
                final byte[] readBytes = new byte[lengthWritten + 1];
                final int lengthSeen = in.read(readBytes);
                assertThat(lengthSeen, equalTo(lengthWritten));
                assertArrayEquals(blobData, Arrays.copyOf(readBytes, lengthWritten));
            }
        }
    }

    public void testReadAfterWriteAfterReadThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
                xContentRegistry(),
                BlobStoreTestUtil.mockClusterService(),
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            expectThrows(NoSuchFileException.class, () -> blobContainer.readBlob(blobName));
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            assertThrowsOnInconsistentRead(blobContainer, blobName);
        }
    }

    public void testReadAfterDeleteAfterWriteThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
                xContentRegistry(),
                BlobStoreTestUtil.mockClusterService(),
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName));
            assertThrowsOnInconsistentRead(blobContainer, blobName);
            blobStoreContext.forceConsistent();
            expectThrows(NoSuchFileException.class, () -> blobContainer.readBlob(blobName));
        }
    }

    public void testOverwriteRandomBlobFails() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
                xContentRegistry(),
                BlobStoreTestUtil.mockClusterService(),
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            repository.start();
            final BlobContainer container = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false);
            final AssertionError assertionError = expectThrows(
                AssertionError.class,
                () -> container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten - 1, false)
            );
            assertThat(assertionError.getMessage(), startsWith("Tried to overwrite blob [" + blobName + "]"));
        }
    }

    public void testOverwriteShardSnapBlobFails() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY),
                xContentRegistry(),
                BlobStoreTestUtil.mockClusterService(),
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            repository.start();
            final BlobContainer container = repository.blobStore()
                .blobContainer(repository.basePath().add("indices").add("someindex").add("0"));
            final String blobName = BlobStoreRepository.SNAPSHOT_PREFIX + UUIDs.randomBase64UUID();
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false);
            final AssertionError assertionError = expectThrows(
                AssertionError.class,
                () -> container.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, false)
            );
            assertThat(assertionError.getMessage(), equalTo("Shard level snap-{uuid} blobs should never be overwritten"));
        }
    }

    public void testOverwriteSnapshotInfoBlob() throws Exception {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        final RepositoryMetadata metadata = new RepositoryMetadata("testRepo", "mockEventuallyConsistent", Settings.EMPTY);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        try (
            BlobStoreRepository repository = new MockEventuallyConsistentRepository(
                metadata,
                xContentRegistry(),
                clusterService,
                recoverySettings,
                blobStoreContext,
                random()
            )
        ) {
            clusterService.addStateApplier(event -> repository.updateState(event.state()));
            // Apply state once to initialize repo properly like RepositoriesService would
            repository.updateState(clusterService.state());
            repository.start();

            // We create a snap- blob for snapshot "foo" in the first generation
            final SnapshotId snapshotId = new SnapshotId("foo", UUIDs.randomBase64UUID());
            PlainActionFuture.<RepositoryData, Exception>get(f ->
            // We try to write another snap- blob for "foo" in the next generation. It fails because the content differs.
            repository.finalizeSnapshot(
                ShardGenerations.EMPTY,
                RepositoryData.EMPTY_REPO_GEN,
                Metadata.EMPTY_METADATA,
                new SnapshotInfo(
                    snapshotId,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    0L,
                    null,
                    1L,
                    5,
                    Collections.emptyList(),
                    true,
                    Collections.emptyMap(),
                    false,
                    0
                ),
                Version.CURRENT,
                Function.identity(),
                Priority.NORMAL,
                f
            ));

            // We try to write another snap- blob for "foo" in the next generation. It fails because the content differs.
            final AssertionError assertionError = expectThrows(
                AssertionError.class,
                () -> PlainActionFuture.<RepositoryData, Exception>get(
                    f -> repository.finalizeSnapshot(
                        ShardGenerations.EMPTY,
                        0L,
                        Metadata.EMPTY_METADATA,
                        new SnapshotInfo(
                            snapshotId,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            0L,
                            null,
                            1L,
                            6,
                            Collections.emptyList(),
                            true,
                            Collections.emptyMap(),
                            false,
                            0
                        ),
                        Version.CURRENT,
                        Function.identity(),
                        Priority.NORMAL,
                        f
                    )
                )
            );
            assertThat(assertionError.getMessage(), equalTo("\nExpected: <6>\n     but: was <5>"));

            // We try to write yet another snap- blob for "foo" in the next generation.
            // It passes cleanly because the content of the blob except for the timestamps.
            PlainActionFuture.<RepositoryData, Exception>get(
                f -> repository.finalizeSnapshot(
                    ShardGenerations.EMPTY,
                    0L,
                    Metadata.EMPTY_METADATA,
                    new SnapshotInfo(
                        snapshotId,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        0L,
                        null,
                        2L,
                        5,
                        Collections.emptyList(),
                        true,
                        Collections.emptyMap(),
                        false,
                        0
                    ),
                    Version.CURRENT,
                    Function.identity(),
                    Priority.NORMAL,
                    f
                )
            );
        }
    }

    private static void assertThrowsOnInconsistentRead(BlobContainer blobContainer, String blobName) {
        final AssertionError assertionError = expectThrows(AssertionError.class, () -> blobContainer.readBlob(blobName));
        assertThat(assertionError.getMessage(), equalTo("Inconsistent read on [" + blobName + ']'));
    }
}
