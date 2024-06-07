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
public abstract class SearchRequestOperationsListener {
    private volatile boolean enabled;
    public static final SearchRequestOperationsListener NOOP = new SearchRequestOperationsListener(false) {
        @Override
        protected void onPhaseStart(SearchPhaseContext context) {}

        @Override
        protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

        @Override
        protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}
    };

    protected SearchRequestOperationsListener() {
        this.enabled = true;
    }

    protected SearchRequestOperationsListener(final boolean enabled) {
        this.enabled = enabled;
    }

    protected void onPhaseStart(SearchPhaseContext context) {};

    protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {};

    protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {};

    protected void onRequestStart(SearchRequestContext searchRequestContext) {}

    protected void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    protected boolean isEnabled(SearchRequest searchRequest) {
        return isEnabled();
    }

    protected boolean isEnabled() {
        return enabled;
    }

    protected void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

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
        protected void onPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseEnd(context, searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onPhaseFailure(context, cause);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        protected void onRequestStart(SearchRequestContext searchRequestContext) {
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

        @Override
        public void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onRequestFailure(context, searchRequestContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onRequestFailure listener [{}] failed", listener), e);
                }
            }
        }

        public List<SearchRequestOperationsListener> getListeners() {
            return listeners;
        }
    }
}
