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

package org.opensearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class })
public class HdfsTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(HdfsPlugin.class);
    }

    public void testSimpleWorkflow() {
        Client client = client();
        Settings.Builder settings = Settings.builder()
            .put("uri", "hdfs:///")
            .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
            .put("path", "foo")
            .put("chunk_size", randomIntBetween(100, 1000) + "k")
            .put("compress", randomBoolean());
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo", "hdfs", settings);

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client.prepareDelete("test-idx-1", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client.prepareDelete("test-idx-2", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client.prepareDelete("test-idx-3", Integer.toString(i)).get();
        }
        client().admin().indices().prepareRefresh().get();
        assertThat(count(client, "test-idx-1"), equalTo(50L));
        assertThat(count(client, "test-idx-2"), equalTo(50L));
        assertThat(count(client, "test-idx-3"), equalTo(50L));

        logger.info("--> close indices");
        client.admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(50L));

        // Test restore after index deletion
        logger.info("--> delete indices");
        client().admin().indices().prepareDelete("test-idx-1", "test-idx-2").get();
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-2")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetadata().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetadata().hasIndex("test-idx-2"), equalTo(false));
        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
        BlobStoreTestUtil.assertConsistency(repo, repo.threadPool().executor(ThreadPool.Names.GENERIC));
    }

    public void testMissingUri() {
        try {
            OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "hdfs", Settings.builder());
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'uri' defined for hdfs"));
        }
    }

    public void testEmptyUri() {
        try {
            Settings.Builder settings = Settings.builder().put("uri", "/path");
            OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "hdfs", settings);
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains("Invalid scheme [null] specified in uri [/path]"));
        }
    }

    public void testNonHdfsUri() {
        try {
            Settings.Builder settings = Settings.builder().put("uri", "file:///");
            OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "hdfs", settings);
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("Invalid scheme [file] specified in uri [file:///]"));
        }
    }

    public void testPathSpecifiedInHdfs() {
        try {
            Settings.Builder settings = Settings.builder().put("uri", "hdfs:///some/path");
            OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "hdfs", settings);
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("Use 'path' option to specify a path [/some/path]"));
        }
    }

    public void testMissingPath() {
        try {
            Settings.Builder settings = Settings.builder().put("uri", "hdfs:///");
            OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "hdfs", settings);
            fail();
        } catch (RepositoryException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No 'path' defined for hdfs"));
        }
    }

    private long count(Client client, String index) {
        return client.prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
    }
}
