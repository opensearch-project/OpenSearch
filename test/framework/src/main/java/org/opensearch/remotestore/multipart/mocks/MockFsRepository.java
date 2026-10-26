/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart.mocks;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.repositories.fs.FsRepository;

public class MockFsRepository extends FsRepository {

    public static Setting<Boolean> TRIGGER_DATA_INTEGRITY_FAILURE = Setting.boolSetting(
        "mock_fs_repository.trigger_data_integrity_failure",
        false
    );

    private final boolean triggerDataIntegrityFailure;

    public MockFsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        this(metadata, environment, namedXContentRegistry, clusterService, recoverySettings, null);
    }

    /**
     * Variant that also wires a native store provider, for tests whose shards need a live
     * {@code NativeStoreRepository} (e.g. warm Parquet shards in the sandbox composite engine).
     * Such tests would otherwise have to substitute a plain {@link FsRepository} for this repo type
     * and would silently lose the mock's behaviour, including the conditional-write support that
     * remote store fencing requires.
     */
    public MockFsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        NativeRemoteObjectStoreProvider nativeStoreProvider
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings, nativeStoreProvider);
        triggerDataIntegrityFailure = TRIGGER_DATA_INTEGRITY_FAILURE.get(metadata.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        FsBlobStore fsBlobStore = (FsBlobStore) super.createBlobStore();
        return new MockFsBlobStore(fsBlobStore.bufferSizeInBytes(), fsBlobStore.path(), isReadOnly(), triggerDataIntegrityFailure);
    }
}
