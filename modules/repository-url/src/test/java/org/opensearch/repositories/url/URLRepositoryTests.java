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

package org.opensearch.repositories.url;

import org.junit.Before;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class URLRepositoryTests extends OpenSearchTestCase {

    @Before
    public void setup() {
        assumeFalse("Test does not run", true);
    }

    private URLRepository createRepository(Settings baseSettings, RepositoryMetadata repositoryMetadata) {
        return new URLRepository(
            repositoryMetadata,
            TestEnvironment.newEnvironment(baseSettings),
            new NamedXContentRegistry(Collections.emptyList()),
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(baseSettings, new ClusterSettings(baseSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually on test/main threads
            }
        };
    }

    public void testAllowListingRepoURL() throws IOException {
        String repoPath = createTempDir().resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.ALLOWED_URLS_SETTING.getKey(), repoPath)
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
        repository.start();

        assertThat("blob store has to be lazy initialized", repository.getBlobStore(), is(nullValue()));
        repository.blobContainer();
        assertThat("blobContainer has to initialize blob store", repository.getBlobStore(), not(nullValue()));
    }

    public void testIfNotAllowListedMustSetRepoURL() throws IOException {
        String repoPath = createTempDir().resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            String msg = "[url] file url ["
                + repoPath
                + "] doesn't match any of the locations specified by path.repo or repositories.url.allowed_urls";
            assertEquals(msg, e.getMessage());
        }
    }

    public void testMustBeSupportedProtocol() throws IOException {
        Path directory = createTempDir();
        String repoPath = directory.resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(Environment.PATH_REPO_SETTING.getKey(), directory.toString())
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .put(URLRepository.SUPPORTED_PROTOCOLS_SETTING.getKey(), "http,https")
            .build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            assertEquals("[url] unsupported url protocol [file] from URL [" + repoPath + "]", e.getMessage());
        }
    }

    public void testNonNormalizedUrl() throws IOException {
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.ALLOWED_URLS_SETTING.getKey(), "file:/tmp/")
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), "file:/var/")
            .build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            assertEquals(
                "[url] file url [file:/var/] doesn't match any of the locations "
                    + "specified by path.repo or repositories.url.allowed_urls",
                e.getMessage()
            );
        }
    }

}
