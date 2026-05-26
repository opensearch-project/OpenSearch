/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs.native_store;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Integration test verifying FsRepository native store lifecycle.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class FsNativeStoreIT extends OpenSearchIntegTestCase {

    private Path repoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FsNativeObjectStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (repoPath == null) {
            repoPath = createTempDir();
        }
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("path.repo", repoPath.toAbsolutePath().toString()).build();
    }

    public void testFsRepoGetsNativeStore() throws IOException {
        Path location = repoPath.resolve("test-repo");
        Files.createDirectories(location);

        AcknowledgedResponse response = client().admin()
            .cluster()
            .preparePutRepository("test-fs-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", location.toAbsolutePath().toString()))
            .get();
        assertTrue("Repository creation should be acknowledged", response.isAcknowledged());

        RepositoriesService repoService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        Repository repo = repoService.repository("test-fs-repo");

        long ptr = repo.getNativeStore().getPointer();
        assertThat("Native store pointer should be > 0", ptr, greaterThan(0L));
        assertEquals("Pointer should be consistent across calls", ptr, repo.getNativeStore().getPointer());
    }

    public void testDifferentReposGetDifferentPointers() throws IOException {
        Path loc1 = repoPath.resolve("repo-1");
        Path loc2 = repoPath.resolve("repo-2");
        Files.createDirectories(loc1);
        Files.createDirectories(loc2);

        client().admin()
            .cluster()
            .preparePutRepository("repo-1")
            .setType("fs")
            .setSettings(Settings.builder().put("location", loc1.toAbsolutePath().toString()))
            .get();
        client().admin()
            .cluster()
            .preparePutRepository("repo-2")
            .setType("fs")
            .setSettings(Settings.builder().put("location", loc2.toAbsolutePath().toString()))
            .get();

        RepositoriesService repoService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        long ptr1 = repoService.repository("repo-1").getNativeStore().getPointer();
        long ptr2 = repoService.repository("repo-2").getNativeStore().getPointer();

        assertThat("Both pointers should be > 0", ptr1, greaterThan(0L));
        assertThat("Both pointers should be > 0", ptr2, greaterThan(0L));
        assertThat("Different repos should get different native store pointers", ptr1, not(equalTo(ptr2)));
    }

    public void testNativeStoreDestroyedOnRepoDelete() throws IOException {
        Path loc = repoPath.resolve("repo-delete");
        Files.createDirectories(loc);

        client().admin()
            .cluster()
            .preparePutRepository("repo-delete")
            .setType("fs")
            .setSettings(Settings.builder().put("location", loc.toAbsolutePath().toString()))
            .get();

        RepositoriesService repoService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        Repository repo = repoService.repository("repo-delete");
        long ptr = repo.getNativeStore().getPointer();
        assertThat("Pointer should be > 0 before delete", ptr, greaterThan(0L));

        client().admin().cluster().prepareDeleteRepository("repo-delete").get();

        expectThrows(IllegalStateException.class, () -> repo.getNativeStore().getPointer());
    }
}
