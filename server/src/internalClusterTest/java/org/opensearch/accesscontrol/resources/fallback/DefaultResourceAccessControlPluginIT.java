/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources.fallback;

import org.opensearch.accesscontrol.resources.ResourceSharing;
import org.opensearch.accesscontrol.resources.ShareWith;
import org.opensearch.accesscontrol.resources.testplugins.TestResourcePlugin;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ResourceAccessControlPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.accesscontrol.resources.testplugins.TestResourcePlugin.SAMPLE_TEST_INDEX;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DefaultResourceAccessControlPluginIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestResourcePlugin.class);
    }

    public void testGetResources() throws IOException {
        final Client client = client();

        createIndex(SAMPLE_TEST_INDEX);
        indexSampleDocuments();

        Set<TestResourcePlugin.TestResource> resources;
        try (
            DefaultResourceAccessControlPlugin plugin = new DefaultResourceAccessControlPlugin(
                client,
                internalCluster().getInstance(ThreadPool.class)
            )
        ) {
            resources = plugin.getAccessibleResourcesForCurrentUser(SAMPLE_TEST_INDEX, TestResourcePlugin.TestResource.class);

            assertNotNull(resources);
            MatcherAssert.assertThat(resources, hasSize(2));

            MatcherAssert.assertThat(resources, hasItem(hasProperty("id", is("1"))));
            MatcherAssert.assertThat(resources, hasItem(hasProperty("id", is("2"))));
        }
    }

    public void testSampleResourcePluginListResources() throws IOException {
        createIndex(SAMPLE_TEST_INDEX);
        indexSampleDocuments();

        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        Set<TestResourcePlugin.TestResource> resources = racPlugin.getAccessibleResourcesForCurrentUser(
            SAMPLE_TEST_INDEX,
            TestResourcePlugin.TestResource.class
        );

        assertNotNull(resources);
        MatcherAssert.assertThat(resources, hasSize(2));
        MatcherAssert.assertThat(resources, hasItem(hasProperty("id", is("1"))));
        MatcherAssert.assertThat(resources, hasItem(hasProperty("id", is("2"))));
    }

    public void testSampleResourcePluginCallsHasPermission() {

        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        boolean canAccess = racPlugin.hasPermission("1", SAMPLE_TEST_INDEX, "some_scope");

        MatcherAssert.assertThat(canAccess, is(true));

    }

    public void testSampleResourcePluginCallsShareWith() {

        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        ResourceSharing sharingInfo = racPlugin.shareWith("1", SAMPLE_TEST_INDEX, new ShareWith(Set.of()));

        MatcherAssert.assertThat(sharingInfo, is(nullValue()));
    }

    public void testSampleResourcePluginCallsRevokeAccess() {

        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        ResourceSharing sharingInfo = racPlugin.revokeAccess("1", SAMPLE_TEST_INDEX, Map.of(), Set.of("some_scope"));

        MatcherAssert.assertThat(sharingInfo, is(nullValue()));
    }

    public void testSampleResourcePluginCallsDeleteResourceSharingRecord() {
        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        boolean recordDeleted = racPlugin.deleteResourceSharingRecord("1", SAMPLE_TEST_INDEX);

        // no record to delete
        MatcherAssert.assertThat(recordDeleted, is(false));
    }

    public void testSampleResourcePluginCallsDeleteAllResourceSharingRecordsForCurrentUser() {
        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), is(DefaultResourceAccessControlPlugin.class));

        boolean recordDeleted = racPlugin.deleteAllResourceSharingRecordsForCurrentUser();

        // no records to delete
        MatcherAssert.assertThat(recordDeleted, is(false));
    }

    private void indexSampleDocuments() throws IOException {
        XContentBuilder doc1 = jsonBuilder().startObject().field("id", "1").field("name", "Test Document 1").endObject();

        XContentBuilder doc2 = jsonBuilder().startObject().field("id", "2").field("name", "Test Document 2").endObject();

        try (Client client = client()) {

            client.prepareIndex(SAMPLE_TEST_INDEX).setId("1").setSource(doc1).get();

            client.prepareIndex(SAMPLE_TEST_INDEX).setId("2").setSource(doc2).get();

            client.admin().indices().prepareRefresh(SAMPLE_TEST_INDEX).get();
        }
    }
}
