/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;

/**
 * Which repositories hand out the full-copy delete entrypoint. The production file-system repositories do not declare it,
 * so they hand out none whatever their store probe concluded: a probe cannot tell the reloadable-fs container's JVM-local
 * emulation from a store that enforces conditional writes, so this, and not the probe, is what keeps such repositories
 * from delete time budgets.
 */
public class DeleteCapabilityDeclarationTests extends OpenSearchTestCase {

    private final RecoverySettings recoverySettings = new RecoverySettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    private final List<BlobStoreRepository> repositories = new ArrayList<>();

    private ThreadPool threadPool;

    private Environment environment;

    private Path repositoryRoot;

    @Before
    public void createEnvironment() {
        threadPool = new TestThreadPool(getTestName());
        final Path home = createTempDir();
        repositoryRoot = home.resolve("repos");
        environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
                .put(Environment.PATH_REPO_SETTING.getKey(), repositoryRoot.toString())
                .build()
        );
    }

    @After
    public void closeRepositories() {
        repositories.forEach(BlobStoreRepository::close);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFileSystemRepositoriesDoNotDeclareTheDeleteCapability() throws Exception {
        for (String type : List.of(FsRepository.TYPE, ReloadableFsRepository.TYPE)) {
            final RepositoryMetadata metadata = metadata(type, Settings.EMPTY);
            final FsRepository repository = type.equals(FsRepository.TYPE)
                ? new FsRepository(metadata, environment, NamedXContentRegistry.EMPTY, clusterService(metadata), recoverySettings)
                : new ReloadableFsRepository(
                    metadata,
                    environment,
                    NamedXContentRegistry.EMPTY,
                    clusterService(metadata),
                    recoverySettings
                );
            repositories.add(repository);
            repository.updateState(BlobStoreTestUtil.mockClusterService(metadata).state());
            setConditionalWriteProof(repository, "PROVEN");
            assertTrue(
                "the probe answer is proven, so a declaring subclass would be handed the entrypoint",
                repository.blobStoreAbandonableSnapshotDelete().isPresent()
            );
            assertTrue("[" + type + "] must not declare the delete capability", repository.abandonableSnapshotDelete().isEmpty());
        }
    }

    private RepositoryMetadata metadata(String type, Settings settings) {
        final Settings withLocation = Settings.builder()
            .put(settings)
            .put(FsRepository.LOCATION_SETTING.getKey(), repositoryRoot.resolve(randomAlphaOfLength(10)).toString())
            .build();
        // A committed generation, so that only the settings under test can make the repository best-effort.
        return new RepositoryMetadata(randomAlphaOfLength(10), type, withLocation, 0L, 0L);
    }

    private ClusterService clusterService(RepositoryMetadata metadata) {
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        when(clusterService.getClusterApplierService().threadPool()).thenReturn(threadPool);
        return clusterService;
    }

    /** Sets the repository's private proof state, as a completed probe would have left it. */
    @SuppressForbidden(reason = "the proof state is private to BlobStoreRepository and must not gain a test seam")
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setConditionalWriteProof(BlobStoreRepository repository, String state) throws Exception {
        final Field field = BlobStoreRepository.class.getDeclaredField("conditionalWriteProof");
        field.setAccessible(true);
        final AtomicReference reference = (AtomicReference) field.get(repository);
        reference.set(Enum.valueOf((Class<? extends Enum>) reference.get().getClass().asSubclass(Enum.class), state));
    }
}
