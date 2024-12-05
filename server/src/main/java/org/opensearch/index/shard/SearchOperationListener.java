/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.transport.TransportRequest;

import java.util.List;

/**
 * An listener for search, fetch and context events.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface SearchOperationListener {

    /**
     * Executed before the query phase is executed
     * @param searchContext the current search context
     */
    default void onPreQueryPhase(SearchContext searchContext) {}

    /**
     * Executed if a query phased failed.
     * @param searchContext the current search context
     */
    default void onFailedQueryPhase(SearchContext searchContext) {}

    /**
     * Executed after the query phase successfully finished.
     * Note: this is not invoked if the query phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the query execution took
     *
     * @see #onFailedQueryPhase(SearchContext)
     */
    default void onQueryPhase(SearchContext searchContext, long tookInNanos) {}

    /**
     * Executed before the slice execution in
     * {@link org.opensearch.search.internal.ContextIndexSearcher#search(List, org.apache.lucene.search.Weight, org.apache.lucene.search.Collector)}.
     * This will be called once per slice in concurrent search and only once in non-concurrent search.
     * @param searchContext the current search context
     */
    default void onPreSliceExecution(SearchContext searchContext) {}

    /**
     * Executed if the slice execution in
     * {@link org.opensearch.search.internal.ContextIndexSearcher#search(List, org.apache.lucene.search.Weight, org.apache.lucene.search.Collector)} failed.
     * This will be called once per slice in concurrent search and only once in non-concurrent search.
     * @param searchContext the current search context
     */
    default void onFailedSliceExecution(SearchContext searchContext) {}

    /**
     * Executed after the slice execution in
     * {@link org.opensearch.search.internal.ContextIndexSearcher#search(List, org.apache.lucene.search.Weight, org.apache.lucene.search.Collector)} successfully finished.
     * This will be called once per slice in concurrent search and only once in non-concurrent search.
     * Note: this is not invoked if the slice execution failed.*
     * @param searchContext the current search context
     *
     * @see #onFailedSliceExecution(org.opensearch.search.internal.SearchContext)
     */
    default void onSliceExecution(SearchContext searchContext) {}

    /**
     * Executed before the fetch phase is executed
     * @param searchContext the current search context
     */
    default void onPreFetchPhase(SearchContext searchContext) {}

    /**
     * Executed if a fetch phased failed.
     * @param searchContext the current search context
     */
    default void onFailedFetchPhase(SearchContext searchContext) {}

    /**
     * Executed after the fetch phase successfully finished.
     * Note: this is not invoked if the fetch phase execution failed.
     * @param searchContext the current search context
     * @param tookInNanos the number of nanoseconds the fetch execution took
     *
     * @see #onFailedFetchPhase(SearchContext)
     */
    default void onFetchPhase(SearchContext searchContext, long tookInNanos) {}

    /**
     * Executed when a new reader context was created
     * @param readerContext the created context
     */
    default void onNewReaderContext(ReaderContext readerContext) {}

    /**
     * Executed when a previously created reader context is freed.
     * This happens either when the search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param readerContext the freed reader context
     */
    default void onFreeReaderContext(ReaderContext readerContext) {}

    /**
     * Executed when a new scroll search {@link ReaderContext} was created
     * @param readerContext the created reader context
     */
    default void onNewScrollContext(ReaderContext readerContext) {}

    /**
     * Executed when a scroll search {@link SearchContext} is freed.
     * This happens either when the scroll search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param readerContext the freed search context
     */
    default void onFreeScrollContext(ReaderContext readerContext) {}

    /**
     * Executed prior to using a {@link ReaderContext} that has been retrieved
     * from the active contexts. If the context is deemed invalid a runtime
     * exception can be thrown, which will prevent the context from being used.
     * @param readerContext The reader context used by this request.
     * @param transportRequest the request that is going to use the search context
     */
    default void validateReaderContext(ReaderContext readerContext, TransportRequest transportRequest) {}

    /**
     * Executed when a new Point-In-Time {@link ReaderContext} was created
     * @param readerContext the created reader context
     */
    default void onNewPitContext(ReaderContext readerContext) {}

    /**
     * Executed when a Point-In-Time search {@link SearchContext} is freed.
     * This happens on deletion of a Point-In-Time or on it's keep-alive is expiring.
     * @param readerContext the freed search context
     */
    default void onFreePitContext(ReaderContext readerContext) {}

    /**
     * Executed when a shard goes from idle to non-idle state
     */
    default void onSearchIdleReactivation() {}

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements SearchOperationListener {
        private final List<SearchOperationListener> listeners;
        private final Logger logger;

        CompositeListener(List<SearchOperationListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        @Override
        public void onPreQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPreQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFailedQueryPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedQueryPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFailedQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onQueryPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onQueryPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onPreSliceExecution(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreSliceExecution(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPreSliceExecution listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFailedSliceExecution(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedSliceExecution(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFailedSliceExecution listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onSliceExecution(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onSliceExecution(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onSliceExecution listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onPreFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onPreFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPreFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFailedFetchPhase(SearchContext searchContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFailedFetchPhase(searchContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFailedFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFetchPhase(searchContext, tookInNanos);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchPhase listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onNewReaderContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewReaderContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onNewContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFreeReaderContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeReaderContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFreeContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onNewScrollContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewScrollContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onNewScrollContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFreeScrollContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreeScrollContext(readerContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFreeScrollContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void validateReaderContext(ReaderContext readerContext, TransportRequest request) {
            Exception exception = null;
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.validateReaderContext(readerContext, request);
                } catch (Exception e) {
                    exception = ExceptionsHelper.useOrSuppress(exception, e);
                }
            }
            ExceptionsHelper.reThrowIfNotNull(exception);
        }

        /**
         * Executed when a new Point-In-Time {@link ReaderContext} was created
         * @param readerContext the created reader context
         */
        @Override
        public void onNewPitContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onNewPitContext(readerContext);
                } catch (Exception e) {
                    logger.warn("onNewPitContext listener failed", e);
                }
            }
        }

        /**
         * Executed when a Point-In-Time search {@link SearchContext} is freed.
         * This happens on deletion of a Point-In-Time or on it's keep-alive is expiring.
         * @param readerContext the freed search context
         */
        @Override
        public void onFreePitContext(ReaderContext readerContext) {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onFreePitContext(readerContext);
                } catch (Exception e) {
                    logger.warn("onFreePitContext listener failed", e);
                }
            }
        }

        @Override
        public void onSearchIdleReactivation() {
            for (SearchOperationListener listener : listeners) {
                try {
                    listener.onSearchIdleReactivation();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onNewSearchIdleReactivation listener [{}] failed", listener), e);
                }
            }
        }
    }
}
