/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.InternalApi;

import java.util.List;

/**
 * A listener for search, fetch and context events at the coordinator node level
 *
 * @opensearch.internal
 */
@InternalApi
interface SearchRequestOperationsListener {

    void onPhaseStart(SearchPhaseContext context);

    void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext);

    void onPhaseFailure(SearchPhaseContext context);

    default void onRequestStart(SearchRequestContext searchRequestContext) {}

    default void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    /**
     * Holder of Composite Listeners
     *
     * @opensearch.internal
     */

    final class CompositeListener implements SearchRequestOperationsListener {
        private final List<SearchRequestOperationsListener> listeners;
        private final Logger logger;

        public CompositeListener(List<SearchRequestOperationsListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public void onPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseEnd(context, searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onRequestStart(SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onRequestStart(searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onRequestStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onRequestEnd(context, searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onRequestEnd listener [{}] failed", listener), e);
                }
            }
        }
    }
}
