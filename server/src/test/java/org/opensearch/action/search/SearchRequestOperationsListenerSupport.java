/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;

import java.util.List;

/**
 * Helper interface to access package protected {@link SearchRequestOperationsListener} from test cases.
 */
public interface SearchRequestOperationsListenerSupport {
    default void onPhaseStart(SearchRequestOperationsListener listener, SearchPhaseContext context) {
        listener.onPhaseStart(context);
    }

    default void onPhaseEnd(SearchRequestOperationsListener listener, SearchPhaseContext context) {
        listener.onPhaseEnd(
            context,
            new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                new SearchRequest(),
                () -> null
            )
        );
    }
}
