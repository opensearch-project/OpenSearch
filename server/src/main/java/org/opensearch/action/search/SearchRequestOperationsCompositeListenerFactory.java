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
 * SearchRequestOperationsCompositeListenerFactory contains listeners registered to search requests,
 * and is responsible for creating the {@link SearchRequestOperationsListener.CompositeListener}
 * with the all listeners enabled at cluster-level and request-level.
 *
 *
 * @opensearch.internal
 */
public final class SearchRequestOperationsCompositeListenerFactory {
    private final List<SearchRequestOperationsListener> searchRequestListenersList;

    /**
     * Create the SearchRequestOperationsCompositeListenerFactory and add multiple {@link SearchRequestOperationsListener}
     * to the searchRequestListenersList.
     * Those enabled listeners will be executed during each search request.
     *
     * @param listeners Multiple SearchRequestOperationsListener object to add.
     * @throws IllegalArgumentException if any input listener is null.
     */
    public SearchRequestOperationsCompositeListenerFactory(final SearchRequestOperationsListener... listeners) {
        searchRequestListenersList = new ArrayList<>();
        for (SearchRequestOperationsListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listener must not be null");
            }
            searchRequestListenersList.add(listener);
        }
    }

    /**
     * Get searchRequestListenersList,
     *
     * @return List of SearchRequestOperationsListener
     */
    public List<SearchRequestOperationsListener> getListeners() {
        return searchRequestListenersList;
    }

    /**
     * Create the {@link SearchRequestOperationsListener.CompositeListener}
     * with the all listeners enabled at cluster-level and request-level.
     *
     * @param searchRequest The SearchRequest object used to decide which request-level listeners to add based on states/flags
     * @param logger Logger to be attached to the {@link SearchRequestOperationsListener.CompositeListener}
     * @param perRequestListeners the per-request listeners that can be optionally added to the returned CompositeListener list.
     * @return SearchRequestOperationsListener.CompositeListener
     */
    public SearchRequestOperationsListener.CompositeListener buildCompositeListener(
        final SearchRequest searchRequest,
        final Logger logger,
        final SearchRequestOperationsListener... perRequestListeners
    ) {
        final List<SearchRequestOperationsListener> searchListenersList = Stream.concat(
            searchRequestListenersList.stream(),
            Arrays.stream(perRequestListeners)

        )
            .filter((searchRequestOperationsListener -> searchRequestOperationsListener.isEnabled(searchRequest)))
            .collect(Collectors.toList());

        return new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger);
    }

}
