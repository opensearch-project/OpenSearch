/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FinalizationCapabilityDeclarationTests extends OpenSearchTestCase {

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFileSystemRepositoriesDoNotDeclareTheFinalizationCapability() throws Exception {
        final Path home = createTempDir();
        final Path repositoryRoot = home.resolve("repos");
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
                .put(Environment.PATH_REPO_SETTING.getKey(), repositoryRoot.toString())
                .build()
        );
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        for (String type : List.of(FsRepository.TYPE, ReloadableFsRepository.TYPE)) {
            final RepositoryMetadata metadata = new RepositoryMetadata(
                "repo-" + type,
                type,
                Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repositoryRoot.resolve(type).toString()).build(),
                0L,
                0L
            );
            final FsRepository repository = type.equals(FsRepository.TYPE)
                ? new FsRepository(
                    metadata,
                    environment,
                    NamedXContentRegistry.EMPTY,
                    BlobStoreTestUtil.mockClusterService(metadata),
                    recoverySettings
                )
                : new ReloadableFsRepository(
                    metadata,
                    environment,
                    NamedXContentRegistry.EMPTY,
                    BlobStoreTestUtil.mockClusterService(metadata),
                    recoverySettings
                );
            try {
                repository.updateState(BlobStoreTestUtil.mockClusterService(metadata).state());
                setConditionalWriteProof(repository, "PROVEN");
                assertTrue(
                    "the probe answer is proven, so a declaring subclass would be handed the entrypoint",
                    repository.blobStoreAbandonableSnapshotFinalization().isPresent()
                );
                assertTrue(
                    "[" + type + "] must not declare the finalization capability",
                    repository.abandonableSnapshotFinalization().isEmpty()
                );
                assertTrue("proven, so a declarer gets it", repository.blobStoreAbandonableSnapshotDelete().isPresent());
                assertTrue("[" + type + "] must not declare the delete capability", repository.abandonableSnapshotDelete().isEmpty());
            } finally {
                repository.close();
            }
        }
    }

    @SuppressForbidden(reason = "the proof state is private to BlobStoreRepository and must not gain a test seam")
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setConditionalWriteProof(BlobStoreRepository repository, String state) throws Exception {
        final Field field = BlobStoreRepository.class.getDeclaredField("conditionalWriteProof");
        field.setAccessible(true);
        final AtomicReference reference = (AtomicReference) field.get(repository);
        reference.set(Enum.valueOf((Class<? extends Enum>) reference.get().getClass().asSubclass(Enum.class), state));
    }
}
