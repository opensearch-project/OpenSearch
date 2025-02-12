/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemRepositoryIT extends AbstractSnapshotIntegTestCase {
    protected Path absolutePath;
    final String systemRepoName = "system-repo-name";

    @Before
    public void setup() {
        absolutePath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(systemRepoName, absolutePath))
            .build();
    }

    public void testRestrictedSettingsCantBeUpdated() {
        disableRepoConsistencyCheck("System repository is being used for the test");

        internalCluster().startNode();
        final Settings.Builder repoSettings = Settings.builder().put("location", randomRepoPath());
        RepositoryException e = expectThrows(RepositoryException.class, () -> createRepository(systemRepoName, "mock", repoSettings));
        assertEquals(
            e.getMessage(),
            "[system-repo-name] trying to modify an unmodifiable attribute type of system "
                + "repository from current value [reloadable-fs] to new value [mock]"
        );
    }

    public void testSystemRepositoryNonRestrictedSettingsCanBeUpdated() {
        disableRepoConsistencyCheck("System repository is being used for the test");
        internalCluster().startNode();
        final Settings.Builder repoSettings = Settings.builder().put("location", absolutePath).put("chunk_size", new ByteSizeValue(20));
        createRepository(systemRepoName, ReloadableFsRepository.TYPE, repoSettings);
    }
}
