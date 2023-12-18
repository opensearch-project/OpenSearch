/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * SearchRequestListenerManager manages listeners registered to search requests,
 * and is responsible for creating the {@link SearchRequestOperationsListener.CompositeListener}
 * with the all listeners enabled at cluster-level and request-level.
 *
 *
 * @opensearch.internal
 */
public class SearchRequestListenerManager {
    private final List<SearchRequestOperationsListener> searchRequestListenersList;

    /**
     * Create the SearchRequestListenerManager and add multiple {@link SearchRequestOperationsListener}
     * to the searchRequestListenersList.
     * Those enabled listeners will be executed during each search request.
     *
     * @param listeners Multiple SearchRequestOperationsListener object to add.
     * @throws IllegalArgumentException if any input listener is null or already exists in the list.
     */
    public SearchRequestListenerManager(SearchRequestOperationsListener... listeners) {
        searchRequestListenersList = new ArrayList<>();
        for (SearchRequestOperationsListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listener must not be null");
            }
            if (searchRequestListenersList.contains(listener)) {
                throw new IllegalArgumentException("listener already added");
            }
            searchRequestListenersList.add(listener);
        }
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
     * @param logger Logger to be attached to the {@link SearchRequestOperationsListener.CompositeListener}
     * @param perRequestListeners the per-request listeners that can be optionally added to the returned CompositeListener list.
     * @return SearchRequestOperationsListener.CompositeListener
     */
    public SearchRequestOperationsListener.CompositeListener buildCompositeListener(
        Logger logger,
        SearchRequestOperationsListener... perRequestListeners
    ) {
        final List<SearchRequestOperationsListener> searchListenersList = Stream.concat(
            searchRequestListenersList.stream(),
            Arrays.stream(perRequestListeners)
        ).filter(SearchRequestOperationsListener::getEnabled).collect(Collectors.toList());

        return new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger);
    }

}
