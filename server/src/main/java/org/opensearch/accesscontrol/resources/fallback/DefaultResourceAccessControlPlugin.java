/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources.fallback;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
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

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This plugin class defines a pass-through implementation of ResourceAccessControlPlugin.
 *
 * @opensearch.experimental
 */
@SuppressWarnings("removal")
public class DefaultResourceAccessControlPlugin extends Plugin implements ResourceAccessControlPlugin {

    private static final Logger log = LogManager.getLogger(DefaultResourceAccessControlPlugin.class);

    private Client client;

    private final ThreadPool threadPool;

    @Inject
    public DefaultResourceAccessControlPlugin(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * Returns a list of all resource ids in provided systemIndex
     * @param resourceIndex index where resources are stored
     *
     * @return Set of resource ids
     */
    @Override
    public <T> Set<T> getAccessibleResourcesForCurrentUser(String resourceIndex, Class<T> clazz) {
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
                parseAndAddToDocuments(Arrays.stream(searchHits).map(SearchHit::getSourceAsMap), clazz, documents);

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
            log.error("Exception: {}", e.getMessage());
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

    private static <T> T parse(Map<String, Object> sourceMap, Class<T> clazz) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            try {
                final T instance = clazz.getDeclaredConstructor().newInstance();
                for (Field field : clazz.getDeclaredFields()) {
                    field.setAccessible(true);
                    Object value = sourceMap.get(field.getName());
                    if (value != null) {
                        field.set(instance, value);
                    }
                }
                return instance;
            } catch (Exception e) {
                throw new OpenSearchException("Failed to parse source map into " + clazz.getName(), e);
            }
        });
    }

    private static <T> void parseAndAddToDocuments(Stream<Map<String, Object>> searchHits, Class<T> clazz, Set<T> documents) {
        searchHits.map(hit -> parse(hit, clazz)).forEach(documents::add);
    }

}
