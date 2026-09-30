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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.mockstore.BlobStoreWrapper;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class ConditionalWriteProofTests extends OpenSearchTestCase {

    private static final TimeValue PROBE_WAIT = TimeValue.timeValueSeconds(30);

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
    public void testOneProbeProvesOnlyAVersioningStoreThreeFailedProbesLeaveItOpenAndExcludedRepositoriesAreNeverProbed() throws Exception {
        for (Store store : List.of(Store.IGNORES_PRECONDITIONS, Store.CONTENT_HASH_VERSIONS, Store.BLIND_TO_PLAIN_WRITES)) {
            final ProbeRepository repository = probeRepository(store, 0);
            timeBudgetsSupported(repository);
            repository.signals.awaitProbe();
            final String expected = store == Store.CONTENT_HASH_VERSIONS ? "PROVEN" : "UNPROVEN";
            assertThat("the probe of a " + store + " store", proof(repository), equalTo(expected));
        }

        final ProbeRepository failingRepository = probeRepository(Store.ENFORCING, Integer.MAX_VALUE);
        for (int probe = 1; probe <= 3; probe++) {
            timeBudgetsSupported(failingRepository);
            failingRepository.signals.awaitProbe();
            assertThat("after failed probe " + probe, proof(failingRepository), equalTo(probe < 3 ? "UNKNOWN" : "UNPROVEN"));
        }
        assertThat(failingRepository.signals.claimChecks.get(), equalTo(3));
        assertFalse(timeBudgetsSupported(failingRepository));

        for (String excluded : List.of(
            BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(),
            BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(),
            BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(),
            BlobStoreRepository.READONLY_SETTING.getKey(),
            BlobStoreRepository.ALLOW_CONCURRENT_MODIFICATION.getKey()
        )) {
            final MockRepository repository = mockRepository(Settings.builder().put(excluded, true).build());
            assertFalse(timeBudgetsSupported(repository));
            assertThat("a repository with [" + excluded + "] must not be probed", proof(repository), equalTo("UNKNOWN"));
            assertThat(repository.conditionalWriteCount(), equalTo(0L));
        }
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { BlobStoreRepository.ALLOW_CONCURRENT_MODIFICATION });
    }

    private RepositoryMetadata metadata(String type, Settings settings) {
        final Settings withLocation = Settings.builder()
            .put(settings)
            .put(FsRepository.LOCATION_SETTING.getKey(), repositoryRoot.resolve(randomAlphaOfLength(10)).toString())
            .build();
        return new RepositoryMetadata(randomAlphaOfLength(10), type, withLocation, 0L, 0L);
    }

    private ClusterService clusterService(RepositoryMetadata metadata) {
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(metadata);
        when(clusterService.getClusterApplierService().threadPool()).thenReturn(threadPool);
        return clusterService;
    }

    private <T extends BlobStoreRepository> T start(T repository) {
        repository.updateState(BlobStoreTestUtil.mockClusterService(repository.getMetadata()).state());
        repository.start();
        repositories.add(repository);
        return repository;
    }

    private ProbeRepository probeRepository(Store store, int failingWrites) {
        final RepositoryMetadata metadata = metadata(FsRepository.TYPE, Settings.EMPTY);
        return start(new ProbeRepository(metadata, environment, clusterService(metadata), recoverySettings, store, failingWrites));
    }

    private MockRepository mockRepository(Settings settings) {
        final RepositoryMetadata metadata = metadata("mock", Settings.builder().put(settings).put("conditional_writes", true).build());
        return start(new MockRepository(metadata, environment, NamedXContentRegistry.EMPTY, clusterService(metadata), recoverySettings));
    }

    @SuppressForbidden(reason = "the capability answer is private to BlobStoreRepository and must not gain a test seam")
    private static boolean timeBudgetsSupported(BlobStoreRepository repository) throws Exception {
        final Method method = BlobStoreRepository.class.getDeclaredMethod("timeBudgetsSupported");
        method.setAccessible(true);
        try {
            return (boolean) method.invoke(repository);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw (Exception) e.getCause();
        }
    }

    @SuppressForbidden(reason = "the proof state is private to BlobStoreRepository and must not gain a test seam")
    private static String proof(BlobStoreRepository repository) throws Exception {
        final Field field = BlobStoreRepository.class.getDeclaredField("conditionalWriteProof");
        field.setAccessible(true);
        return String.valueOf(((AtomicReference<?>) field.get(repository)).get());
    }

    private enum Store {
        ENFORCING,
        IGNORES_PRECONDITIONS,
        CONTENT_HASH_VERSIONS,
        BLIND_TO_PLAIN_WRITES
    }

    private static final class ProbeSignals {

        private final AtomicInteger claimChecks = new AtomicInteger();

        private final Semaphore probesDone = new Semaphore(0);

        /** Guarded by itself. */
        private final Map<String, Long> versions = new HashMap<>();

        private long nextVersion;

        void awaitProbe() throws InterruptedException {
            assertTrue("no probe completed", probesDone.tryAcquire(PROBE_WAIT.millis(), TimeUnit.MILLISECONDS));
        }
    }

    private static BlobStore wrap(BlobStore blobStore, Store store, AtomicInteger failingWrites, ProbeSignals signals) {
        return new BlobStoreWrapper(blobStore) {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new StoreContainer(super.blobContainer(path), store, failingWrites, signals);
            }
        };
    }

    private static final class StoreContainer extends FilterBlobContainer {

        private final BlobContainer inner;

        private final Store store;

        private final AtomicInteger failingWrites;

        private final ProbeSignals signals;

        StoreContainer(BlobContainer inner, Store store, AtomicInteger failingWrites, ProbeSignals signals) {
            super(inner);
            this.inner = inner;
            this.store = store;
            this.failingWrites = failingWrites;
            this.signals = signals;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new StoreContainer(child, store, failingWrites, signals);
        }

        private boolean isProbeContainer() {
            final String[] parts = path().toArray();
            return parts.length > 0 && parts[parts.length - 1].startsWith("tests-");
        }

        private String key(String blobName) {
            return path().buildAsString() + blobName;
        }

        /** Under the signals' monitor: the token a read of the blob with this content returns. */
        private String readToken(String blobName, byte[] content) {
            if (store == Store.CONTENT_HASH_VERSIONS) {
                return MessageDigests.toHexString(MessageDigests.md5().digest(content));
            }
            return "v" + signals.versions.getOrDefault(key(blobName), 0L);
        }

        private void bump(String blobName) {
            synchronized (signals.versions) {
                signals.versions.put(key(blobName), ++signals.nextVersion);
            }
        }

        @Override
        public boolean isConditionalWriteSupported() {
            if (isProbeContainer()) {
                signals.claimChecks.incrementAndGet();
            }
            return true;
        }

        @Override
        public VersionedBlob readBlobWithVersion(String blobName) throws IOException {
            synchronized (signals.versions) {
                final byte[] content;
                try (InputStream stream = inner.readBlob(blobName)) {
                    content = stream.readAllBytes();
                }
                return new VersionedBlob(content, readToken(blobName, content));
            }
        }

        @Override
        public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, String expectedVersionToken)
            throws IOException {
            if (failingWrites.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                throw new IOException("injected conditional write failure");
            }
            final byte[] content = inputStream.readAllBytes();
            synchronized (signals.versions) {
                if (store != Store.IGNORES_PRECONDITIONS) {
                    String current = null;
                    if (inner.blobExists(blobName)) {
                        try (InputStream stream = inner.readBlob(blobName)) {
                            current = readToken(blobName, stream.readAllBytes());
                        }
                    }
                    if (Objects.equals(expectedVersionToken, current) == false) {
                        throw new BlobVersionConflictException("[" + blobName + "] does not have version [" + expectedVersionToken + "]");
                    }
                }
                inner.writeBlobAtomic(blobName, new ByteArrayInputStream(content), content.length, false);
                bump(blobName);
                return readToken(blobName, content);
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            synchronized (signals.versions) {
                inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                if (store != Store.BLIND_TO_PLAIN_WRITES) {
                    bump(blobName);
                }
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            synchronized (signals.versions) {
                inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                if (store != Store.BLIND_TO_PLAIN_WRITES) {
                    bump(blobName);
                }
            }
        }

        @Override
        public DeleteResult delete() throws IOException {
            final DeleteResult result = super.delete();
            if (isProbeContainer()) {
                signals.probesDone.release();
            }
            return result;
        }
    }

    private static final class ProbeRepository extends FsRepository {

        private final Store store;

        private final AtomicInteger failingWrites;

        private final ProbeSignals signals = new ProbeSignals();

        ProbeRepository(
            RepositoryMetadata metadata,
            Environment environment,
            ClusterService clusterService,
            RecoverySettings recoverySettings,
            Store store,
            int failingWrites
        ) {
            super(metadata, environment, NamedXContentRegistry.EMPTY, clusterService, recoverySettings);
            this.store = store;
            this.failingWrites = new AtomicInteger(failingWrites);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return wrap(super.createBlobStore(), store, failingWrites, signals);
        }
    }
}
