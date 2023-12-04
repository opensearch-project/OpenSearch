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
abstract class SearchRequestOperationsListener {

    abstract void onPhaseStart(SearchPhaseContext context);

    abstract void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext);

    abstract void onPhaseFailure(SearchPhaseContext context);

    void onRequestStart(SearchRequestContext searchRequestContext) {}

    void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    /**
     * Holder of Composite Listeners
     *
     * @opensearch.internal
     */

    static final class CompositeListener extends SearchRequestOperationsListener {
        private final List<SearchRequestOperationsListener> listeners;
        private final Logger logger;

        CompositeListener(List<SearchRequestOperationsListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        void onPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseEnd(context, searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        void onPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        void onRequestStart(SearchRequestContext searchRequestContext) {
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
