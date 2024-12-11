/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.accesscontrol.resources.EntityType;
import org.opensearch.accesscontrol.resources.ResourceSharing;
import org.opensearch.accesscontrol.resources.ShareWith;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This plugin class defines a no-op implementation of Resource Plugin.
 *
 * @opensearch.experimental
 */
public class NoOpResourceAccessControlPlugin extends Plugin implements ResourceAccessControlPlugin {

    private Client client;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client = client;
        return List.of();
    }

    /**
     * Returns a list of all resource ids in provided systemIndex
     * @param resourceIndexName index where resources are stored
     *
     * @return Set of resource ids
     */
    @Override
    public Set<String> getAccessibleResourcesForCurrentUser(String resourceIndexName) {
        // TODO: check whether this should return '*'.
        // This would indicate to plugins that user has access to all resources since security is disabled
        // and would eliminate the need for all the code below
        // return Set.of("*");

        SearchRequest searchRequest = new SearchRequest(resourceIndexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(10000);

        searchRequest.scroll(TimeValue.timeValueMinutes(1));
        searchRequest.source(sourceBuilder);

        Set<String> allDocumentIds = new HashSet<>();

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {
            Arrays.stream(searchHits).map(SearchHit::getId).forEach(allDocumentIds::add);

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(1));
            searchResponse = client.execute(SearchScrollAction.INSTANCE, scrollRequest).actionGet();
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest);

        return allDocumentIds;
    }

    /**
     * @param resourceId the resource on which access is to be checked
     * @param resourceIndex where the resource exists
     * @param scope the type of access being requested
     * @return true since security plugin is disabled in the cluster
     */
    @Override
    public boolean hasPermission(String resourceId, String resourceIndex, String scope) {
        return true;
    }

    /**
     * @param resourceId if of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @param shareWith a map that contains entries of entities with whom this resource should be shared with
     * @return null since security plugin is disabled in the cluster
     */
    @Override
    public ResourceSharing shareWith(String resourceId, String resourceIndex, ShareWith shareWith) {
        return null;
    }

    /**
     * @param resourceId if of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @param scopes a list of scopes to be checked for revoking access. If empty, all scopes will be checked.
     * @return null since security plugin is disabled in the cluster
     */
    @Override
    public ResourceSharing revokeAccess(
        String resourceId,
        String resourceIndex,
        Map<EntityType, Set<String>> revokeAccess,
        Set<String> scopes
    ) {
        return null;
    }

    /**
     * @param resourceId if of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @return false since security plugin is disabled
     */
    @Override
    public boolean deleteResourceSharingRecord(String resourceId, String resourceIndex) {
        return false;
    }

    /**
     * @return false since security plugin is disabled
     */
    @Override
    public boolean deleteAllResourceSharingRecordsForCurrentUser() {
        return false;
    }

}
