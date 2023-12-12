/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * SearchRequestListenerManager manages listeners registered to search requests,
 * and is responsible for creating the {@link SearchRequestOperationsListener.CompositeListener}
 * with the all listeners enabled at cluster-level and request-level.
 *
 *
 * @opensearch.internal
 */
public class SearchRequestListenerManager {

    private final ClusterService clusterService;
    public static final String SEARCH_PHASE_TOOK_ENABLED_KEY = "search.phase_took_enabled";
    public static final Setting<Boolean> SEARCH_PHASE_TOOK_ENABLED = Setting.boolSetting(
        SEARCH_PHASE_TOOK_ENABLED_KEY,
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private final List<SearchRequestOperationsListener> searchRequestListenersList;

    @Inject
    public SearchRequestListenerManager(
        ClusterService clusterService
    ) {
        this.clusterService = clusterService;
        searchRequestListenersList = new ArrayList<>();
    }

    /**
     * Add a {@link SearchRequestOperationsListener} to the searchRequestListenersList,
     * which will be executed during each search request.
     *
     * @param listener A SearchRequestOperationsListener object to add.
     * @throws IllegalArgumentException if the input listener is null or already exists in the list.
     */
    public void addListener(SearchRequestOperationsListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (searchRequestListenersList.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }
        searchRequestListenersList.add(listener);
    }

    /**
     * Remove a {@link SearchRequestOperationsListener} from the searchRequestListenersList,
     *
     * @param listener A SearchRequestOperationsListener object to remove.
     * @throws IllegalArgumentException if the input listener is null or already exists in the list.
     */
    public void removeListener(SearchRequestOperationsListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (!searchRequestListenersList.contains(listener)) {
            throw new IllegalArgumentException("listener does not exist in the listeners list");
        }
        searchRequestListenersList.remove(listener);
    }

    /**
     * Get searchRequestListenersList,
     *
     * @return List of SearchRequestOperationsListener
     * @throws IllegalArgumentException if the input listener is null or already exists in the list.
     */
    public List<SearchRequestOperationsListener> getListeners() {
        return searchRequestListenersList;
    }


    /**
     * Create the {@link SearchRequestOperationsListener.CompositeListener}
     * with the all listeners enabled at cluster-level and request-level.
     *
     * @param searchRequest The SearchRequest object. SearchRequestListenerManager will decide which request-level listeners to add based on states/flags of the request
     * @param logger Logger to be attached to the {@link SearchRequestOperationsListener.CompositeListener}
     * @param perRequestListeners the per-request listeners that can be optionally added to the returned CompositeListener list.
     * @return SearchRequestOperationsListener.CompositeListener
     */
    public SearchRequestOperationsListener.CompositeListener buildCompositeListener(
        SearchRequest searchRequest,
        Logger logger,
        SearchRequestOperationsListener... perRequestListeners
    ) {
        final List<SearchRequestOperationsListener> searchListenersList = new ArrayList<>(searchRequestListenersList);

        Arrays.stream(perRequestListeners).parallel().forEach((listener) -> {
            if (listener != null && listener.getClass() == TransportSearchAction.SearchTimeProvider.class) {
                TransportSearchAction.SearchTimeProvider timeProvider = (TransportSearchAction.SearchTimeProvider) listener;
                // phase_took is enabled with request param and/or cluster setting
                boolean phaseTookEnabled = (searchRequest.isPhaseTook() != null && searchRequest.isPhaseTook()) ||
                    clusterService.getClusterSettings().get(SEARCH_PHASE_TOOK_ENABLED);
                if (phaseTookEnabled) {
                    timeProvider.setPhaseTook(true);
                    searchListenersList.add(timeProvider);
                }
            }
        });
        return new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger);
    }

}
