/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs;

import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Random;

/**
 * Extension of {@link FsRepository} that can be reloaded inplace , supports failing operation and slowing it down
 *
 * @opensearch.internal
 */
public class ReloadableFsRepository extends FsRepository {
    public static final String TYPE = "reloadable-fs";

    private final FailSwitch fail;
    private final SlowDownWriteSwitch slowDown;

    public static final Setting<Integer> REPOSITORIES_FAILRATE_SETTING = Setting.intSetting(
        "repositories.fail.rate",
        0,
        0,
        100,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> REPOSITORIES_SLOWDOWN_SETTING = Setting.intSetting(
        "repositories.slowdown",
        0,
        0,
        100,
        Setting.Property.NodeScope
    );

    /**
     * Constructs a shared file system repository that is reloadable in-place.
     */
    public ReloadableFsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        fail = new FailSwitch();
        fail.failRate(REPOSITORIES_FAILRATE_SETTING.get(metadata.settings()));
        slowDown = new SlowDownWriteSwitch();
        slowDown.setSleepSeconds(REPOSITORIES_SLOWDOWN_SETTING.get(metadata.settings()));
        readRepositoryMetadata();
    }

    @Override
    public boolean isReloadable() {
        return true;
    }

    @Override
    public void reload(RepositoryMetadata repositoryMetadata) {
        super.reload(repositoryMetadata);
        readRepositoryMetadata();
        validateLocation();
        readMetadata();
    }

    private void readRepositoryMetadata() {
        fail.failRate(REPOSITORIES_FAILRATE_SETTING.get(metadata.settings()));
        slowDown.setSleepSeconds(REPOSITORIES_SLOWDOWN_SETTING.get(metadata.settings()));
    }

    protected BlobStore createBlobStore() throws Exception {
        final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
        final Path locationFile = environment.resolveRepoFile(location);
        return new ThrowingBlobStore(bufferSize, locationFile, isReadOnly(), fail, slowDown);
    }

    // A random integer from min-max (inclusive).
    public static int randomIntBetween(int min, int max) {
        Random random = Randomness.get();
        return random.nextInt(max - min + 1) + min;
    }

    static class FailSwitch {
        private volatile int failRate;
        private volatile boolean onceFailedFailAlways = false;

        public boolean fail() {
            final int rnd = randomIntBetween(1, 100);
            boolean fail = rnd <= failRate;
            if (fail && onceFailedFailAlways) {
                failAlways();
            }
            return fail;
        }

        public void failAlways() {
            failRate = 100;
        }

        public void failRate(int rate) {
            failRate = rate;
        }

        public void onceFailedFailAlways() {
            onceFailedFailAlways = true;
        }
    }

    static class SlowDownWriteSwitch {
        private volatile int sleepSeconds;

        public void setSleepSeconds(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        public int getSleepSeconds() {
            return sleepSeconds;
        }
    }

    private static class ThrowingBlobStore extends FsBlobStore {

        private final FailSwitch fail;
        private final SlowDownWriteSwitch slowDown;

        public ThrowingBlobStore(int bufferSizeInBytes, Path path, boolean readonly, FailSwitch fail, SlowDownWriteSwitch slowDown)
            throws IOException {
            super(bufferSizeInBytes, path, readonly);
            this.fail = fail;
            this.slowDown = slowDown;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            try {
                return new ThrowingBlobContainer(this, path, buildAndCreate(path), fail, slowDown);
            } catch (IOException ex) {
                throw new OpenSearchException("failed to create blob container", ex);
            }
        }
    }

    private static class ThrowingBlobContainer extends FsBlobContainer {

        private final FailSwitch fail;
        private final SlowDownWriteSwitch slowDown;

        public ThrowingBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, FailSwitch fail, SlowDownWriteSwitch slowDown) {
            super(blobStore, blobPath, path);
            this.fail = fail;
            this.slowDown = slowDown;
        }

        @Override
        public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            checkFailRateAndSleep(blobName);
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        private void checkFailRateAndSleep(String blobName) throws IOException {
            if (fail.fail() && blobName.contains(".dat") == false) {
                throw new IOException("blob container throwing error");
            }
            if (slowDown.getSleepSeconds() > 0) {
                try {
                    Thread.sleep(slowDown.getSleepSeconds() * 1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            checkFailRateAndSleep(blobName);
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }
}
