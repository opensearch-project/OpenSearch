/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.native_store;

import org.opensearch.Version;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.s3.S3RepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test verifying that S3Repository gets a native (Rust) ObjectStore
 * when the native-repository-s3 sandbox plugin is loaded via ExtensiblePlugin.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sandbox:plugins:native-repository-s3:internalClusterTest \
 *   --tests "*.S3NativeStoreIT" -Dsandbox.enabled=true
 * </pre>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class S3NativeStoreIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(S3RepositoryPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                S3NativeObjectStorePlugin.class.getName(),
                "native S3 object store provider",
                "NA",
                Version.CURRENT,
                "1.8",
                S3NativeObjectStorePlugin.class.getName(),
                null,
                List.of(S3RepositoryPlugin.class.getName()),
                false
            )
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "test_access_key");
        secureSettings.setString("s3.client.default.secret_key", "test_secret_key");
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("s3.client.default.endpoint", "http://localhost:12345")
            .put("s3.client.default.region", "us-east-1")
            .setSecureSettings(secureSettings)
            .build();
    }

    public void testS3RepoGetsNativeStoreViaExtensiblePlugin() {
        AcknowledgedResponse response = client().admin()
            .cluster()
            .preparePutRepository("test-s3-repo")
            .setType("s3")
            .setVerify(false)
            .setSettings(Settings.builder().put("bucket", "test-bucket"))
            .get();
        assertTrue("Repository creation should be acknowledged", response.isAcknowledged());

        RepositoriesService repoService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        Repository repo = repoService.repository("test-s3-repo");

        long ptr = repo.getNativeStore().getPointer();
        assertThat("Native store pointer should be > 0", ptr, greaterThan(0L));

        // Pointer should be stable across calls
        assertEquals("Pointer should be consistent across calls", ptr, repo.getNativeStore().getPointer());
    }
}
