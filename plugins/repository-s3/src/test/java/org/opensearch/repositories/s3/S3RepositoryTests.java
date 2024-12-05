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

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.S3Client;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class S3RepositoryTests extends OpenSearchTestCase implements ConfigPathSupport {

    private static class DummyS3Service extends S3Service {
        DummyS3Service(final Path configPath) {
            super(configPath);
        }

        @Override
        public AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
            return new AmazonS3Reference(S3Client.create());
        }

        @Override
        public void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {}

        @Override
        public void close() {}
    }

    public void testInvalidChunkBufferSizeSettings() {
        // chunk < buffer should fail
        final Settings s1 = bufferAndChunkSettings(10, 5);
        final Exception e1 = expectThrows(RepositoryException.class, () -> createS3Repo(getRepositoryMetadata(s1)));
        assertThat(e1.getMessage(), containsString("chunk_size (5mb) can't be lower than buffer_size (10mb)"));
        // chunk > buffer should pass
        final Settings s2 = bufferAndChunkSettings(5, 10);
        createS3Repo(getRepositoryMetadata(s2)).close();
        // chunk = buffer should pass
        final Settings s3 = bufferAndChunkSettings(5, 5);
        createS3Repo(getRepositoryMetadata(s3)).close();
        // buffer < 5mb should fail
        final Settings s4 = bufferAndChunkSettings(4, 10);
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s4)).close()
        );
        assertThat(e2.getMessage(), containsString("failed to parse value [4mb] for setting [buffer_size], must be >= [5mb]"));
        final Settings s5 = bufferAndChunkSettings(5, 6000000);
        final IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s5)).close()
        );
        assertThat(e3.getMessage(), containsString("failed to parse value [6000000mb] for setting [chunk_size], must be <= [5tb]"));
    }

    private Settings bufferAndChunkSettings(long buffer, long chunk) {
        return Settings.builder()
            .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), new ByteSizeValue(buffer, ByteSizeUnit.MB).getStringRep())
            .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunk, ByteSizeUnit.MB).getStringRep())
            .build();
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        return new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(settings).build());
    }

    public void testBasePathSetting() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder().put(S3Repository.BASE_PATH_SETTING.getKey(), "foo/bar").build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertEquals("foo/bar/", s3repo.basePath().buildAsString());
        }
    }

    public void testDefaultBufferSize() {
        Settings settings = Settings.builder().build();
        final RepositoryMetadata metadata = new RepositoryMetadata("dummy-repo", "mock", settings);
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertThat(s3repo.getBlobStore(), is(nullValue()));
            s3repo.start();
            final long defaultBufferSize = ((S3BlobStore) s3repo.blobStore()).bufferSizeInBytes();
            assertThat(s3repo.getBlobStore(), not(nullValue()));
            assertThat(defaultBufferSize, Matchers.lessThanOrEqualTo(100L * 1024 * 1024));
            assertThat(defaultBufferSize, Matchers.greaterThanOrEqualTo(5L * 1024 * 1024));
        }
    }

    public void testIsReloadable() {
        final RepositoryMetadata metadata = new RepositoryMetadata("dummy-repo", "mock", Settings.EMPTY);
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertTrue(s3repo.isReloadable());
        }
    }

    public void testRestrictedSettingsDefault() {
        final RepositoryMetadata metadata = new RepositoryMetadata("dummy-repo", "mock", Settings.EMPTY);
        try (S3Repository s3repo = createS3Repo(metadata)) {
            List<Setting<?>> restrictedSettings = s3repo.getRestrictedSystemRepositorySettings();
            assertThat(restrictedSettings.size(), is(5));
            assertTrue(restrictedSettings.contains(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING));
            assertTrue(restrictedSettings.contains(BlobStoreRepository.READONLY_SETTING));
            assertTrue(restrictedSettings.contains(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY));
            assertTrue(restrictedSettings.contains(S3Repository.BUCKET_SETTING));
            assertTrue(restrictedSettings.contains(S3Repository.BASE_PATH_SETTING));
        }
    }

    private S3Repository createS3Repo(RepositoryMetadata metadata) {
        return new S3Repository(
            metadata,
            NamedXContentRegistry.EMPTY,
            new DummyS3Service(configPath()),
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            null,
            null,
            null,
            null,
            null,
            false,
            null,
            null,
            null,
            null
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually on test/main threads
            }
        };
    }
}
