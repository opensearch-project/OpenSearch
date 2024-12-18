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

import software.amazon.awssdk.services.s3.model.StorageClass;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class S3RepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    @Before
    @SuppressForbidden(reason = "Need to set system property here for AWS SDK v2")
    public void setUp() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", "config"));
        super.setUp();
    }

    @Override
    @SuppressForbidden(reason = "Need to reset system property here for AWS SDK v2")
    public void tearDown() throws Exception {
        SocketAccess.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        super.tearDown();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.s3.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.s3.bucket"))
            .put("region", System.getProperty("test.s3.region", "us-west-2"))
            .put("base_path", System.getProperty("test.s3.base", "testpath"));
        final String endpoint = System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings.put("endpoint", endpoint);
        } else {
            // only test different storage classes when running against the default endpoint, i.e. a genuine S3 service
            if (randomBoolean()) {
                final String storageClass = randomFrom(
                    StorageClass.STANDARD,
                    StorageClass.REDUCED_REDUNDANCY,
                    StorageClass.STANDARD_IA,
                    StorageClass.ONEZONE_IA,
                    StorageClass.INTELLIGENT_TIERING
                ).toString();
                logger.info("--> using storage_class [{}]", storageClass);
                settings.put("storage_class", storageClass);
            }
        }
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "s3", settings);
    }

    @Override
    protected boolean assertCorruptionVisible(BlobStoreRepository repo, Executor genericExec) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> assertTrue(super.assertCorruptionVisible(repo, genericExec)), 10L, TimeUnit.MINUTES);
        return true;
    }

    @Override
    protected void assertConsistentRepository(BlobStoreRepository repo, Executor executor) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> super.assertConsistentRepository(repo, executor), 10L, TimeUnit.MINUTES);
    }

    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetadata> blobs) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertBlobsByPrefix(path, prefix, blobs), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertChildren(path, children), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertDeleted(BlobPath path, String name) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertDeleted(path, name), 10L, TimeUnit.MINUTES);
    }
}
