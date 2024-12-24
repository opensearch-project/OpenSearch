/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources.fallback;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.accesscontrol.resources.Resource;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ResourceAccessControlPlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This plugin class defines a pass-through implementation of ResourceAccessControlPlugin.
 * This plugin will come into effect when security plugin is disabled in the cluster.
 *
 * @opensearch.experimental
 */
public class DefaultResourceAccessControlPlugin extends Plugin implements ResourceAccessControlPlugin {

    private static final Logger log = LogManager.getLogger(DefaultResourceAccessControlPlugin.class);

    private final Client client;

    private final ThreadPool threadPool;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public DefaultResourceAccessControlPlugin(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * Returns a list of all resource ids in provided resourceIndex
     * @param resourceIndex index where resources are stored
     *
     * @return Set of resource ids
     */
    @Override
    public <T extends Resource> Set<T> getAccessibleResourcesForCurrentUser(String resourceIndex, Class<T> clazz) {
        final Set<T> documents = new HashSet<>();
        final TimeValue scrollTimeout = TimeValue.timeValueMinutes(1);
        String scrollId;

        log.info("Searching for accessible resources in index {}", resourceIndex);
        // stashContext is required in case resourceIndex is a system index
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            final SearchRequest searchRequest = buildSearchRequest(resourceIndex, scrollTimeout);

            SearchResponse searchResponse = client.search(searchRequest).actionGet();
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                parseAndAddToDocuments(Arrays.stream(searchHits), clazz, documents);

                final SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scrollTimeout);
                searchResponse = client.execute(SearchScrollAction.INSTANCE, scrollRequest).actionGet();
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest).actionGet();
        } catch (Exception e) {
            log.error("Error fetching resources : {}", e.getMessage());
            return Set.of();
        }
        log.info("Found {} documents", documents.size());
        return Collections.unmodifiableSet(documents);
    }

    private SearchRequest buildSearchRequest(String resourceIndex, TimeValue scrollTimeout) {
        final SearchRequest searchRequest = new SearchRequest(resourceIndex);
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.fetchSource(true);
        sourceBuilder.size(10000);

        searchRequest.scroll(scrollTimeout);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    private static <T> T parse(String source, Class<T> clazz) {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> objectMapper.readValue(source, clazz));
        } catch (PrivilegedActionException e) {
            log.error("Error parsing source: {}", e.toString());
            throw new OpenSearchException("Error parsing source into : " + clazz.getName(), e.getException());
        }
    }

    private static <T> void parseAndAddToDocuments(Stream<SearchHit> searchHits, Class<T> clazz, Set<T> documents) {
        searchHits.map(hit -> parse(hit.getSourceAsString(), clazz)).forEach(documents::add);
    }

}
