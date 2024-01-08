/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.compress.ZstdCompressor;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Locale;

public class ReloadableFsRepositoryTests extends OpenSearchTestCase {
    ReloadableFsRepository repository;
    RepositoryMetadata metadata;
    Settings settings;
    Path repo;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        repo = createTempDir();
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", false)
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
        repository = new ReloadableFsRepository(
            metadata,
            new Environment(settings, null),
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    /**
     * Validates that {@link ReloadableFsRepository} supports inplace reloading
     */
    public void testIsReloadable() {
        assertTrue(repository.isReloadable());
    }

    /**
     * Updates repository metadata of an existing repository to enable default compressor
     */
    public void testCompressReload() {
        assertEquals(CompressorRegistry.none(), repository.getCompressor());
        updateCompressionTypeToDefault();
        repository.validateMetadata(metadata);
        repository.reload(metadata);
        assertEquals(CompressorRegistry.defaultCompressor(), repository.getCompressor());
    }

    /**
     * Updates repository metadata of an existing repository to change compressor type from default to Zstd
     */
    public void testCompressionTypeReload() {
        assertEquals(CompressorRegistry.none(), repository.getCompressor());
        updateCompressionTypeToDefault();
        repository = new ReloadableFsRepository(
            metadata,
            new Environment(settings, null),
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        assertEquals(CompressorRegistry.defaultCompressor(), repository.getCompressor());

        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", true)
            .put("compression_type", ZstdCompressor.NAME.toLowerCase(Locale.ROOT))
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
        repository.validateMetadata(metadata);
        repository.reload(metadata);
        assertEquals(CompressorRegistry.getCompressor(ZstdCompressor.NAME.toUpperCase(Locale.ROOT)), repository.getCompressor());
    }

    private void updateCompressionTypeToDefault() {
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repo)
            .put("compress", true)
            .put("compression_type", DeflateCompressor.NAME.toLowerCase(Locale.ROOT))
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
            .build();
        metadata = new RepositoryMetadata("test", "fs", settings);
    }
}
