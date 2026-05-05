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

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.secure_sm.AccessController;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class S3ServiceTests extends AbstractS3RepositoryTestCase {
    public void testCachedClientsAreReleased() {
        final S3Service s3Service = new S3Service(configPath());
        final Settings settings = Settings.builder().put("endpoint", "http://first").put("region", "region").build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final S3ClientSettings clientSettings = s3Service.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3Service.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonS3Reference reference = AccessController.doPrivileged(() -> s3Service.client(metadata1));
        reference.close();
        s3Service.close();
        final AmazonS3Reference referenceReloaded = AccessController.doPrivileged(() -> s3Service.client(metadata1));
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3Service.close();
        final S3ClientSettings clientSettingsReloaded = s3Service.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }

    public void testCachedClientsWithCredentialsAreReleased() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        final Map<String, S3ClientSettings> defaults = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).put("s3.client.default.identity_token_file", "file").build(),
            configPath()
        );
        final S3Service s3Service = new S3Service(configPath());
        s3Service.refreshAndClearCache(defaults);
        final Settings settings = Settings.builder().put("endpoint", "http://first").put("region", "us-east-2").build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final S3ClientSettings clientSettings = s3Service.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3Service.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonS3Reference reference = AccessController.doPrivileged(() -> s3Service.client(metadata1));
        reference.close();
        s3Service.close();
        final AmazonS3Reference referenceReloaded = AccessController.doPrivileged(() -> s3Service.client(metadata1));
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3Service.close();
        final S3ClientSettings clientSettingsReloaded = s3Service.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }

    public void testResolveEndpointOverrideAbsentWhenEndpointNotProvided() {
        final S3Service s3Service = new S3Service(configPath());
        final Settings repoSettings = Settings.builder()
            // region is required by S3Service.settings(...) in this test suite
            .put("region", "us-east-1")
            // intentionally omit "endpoint"
            .build();
        final RepositoryMetadata metadata = new RepositoryMetadata("no-endpoint", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3Service.settings(metadata);
        final Optional<URI> override = s3Service.resolveEndpointOverride(clientSettings);
        assertTrue("Expected no endpoint override when endpoint setting is absent", override.isEmpty());
    }

    public void testResolveEndpointOverrideAddsSchemeWhenMissing() {
        final S3Service s3Service = new S3Service(configPath());
        final Settings repoSettings = Settings.builder()
            .put("region", "us-east-1")
            // no scheme on purpose
            .put("endpoint", "s3.us-east-1.amazonaws.com")
            .build();
        final RepositoryMetadata metadata = new RepositoryMetadata("endpoint-no-scheme", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3Service.settings(metadata);
        final Optional<URI> override = s3Service.resolveEndpointOverride(clientSettings);
        assertTrue("Expected endpoint override to be present when endpoint setting is provided", override.isPresent());

        // S3 client settings default protocol in tests should be https unless explicitly configured otherwise.
        // If this ever changes in the future, update this assertion accordingly.
        assertEquals("https://s3.us-east-1.amazonaws.com", override.get().toString());
    }

    public void testResolveEndpointOverridePreservesExplicitScheme() {
        final S3Service s3Service = new S3Service(configPath());
        final Settings repoSettings = Settings.builder().put("region", "us-east-1").put("endpoint", "http://localhost:9000").build();
        final RepositoryMetadata metadata = new RepositoryMetadata("endpoint-with-scheme", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3Service.settings(metadata);
        final Optional<URI> override = s3Service.resolveEndpointOverride(clientSettings);
        assertTrue("Expected endpoint override to be present when endpoint has explicit scheme", override.isPresent());
        assertEquals("http://localhost:9000", override.get().toString());
    }
}
