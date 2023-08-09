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

import java.util.List;

/**
 * A listener for search, fetch and context events at the coordinator node level
 *
 * @opensearch.internal
 */
public interface SearchRequestOperationsListener {

    void onDFSPreQueryPhaseStart(SearchPhaseContext context);

    void onDFSPreQueryPhaseFailure(SearchPhaseContext context);

    void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime);

    void onCanMatchPhaseStart(SearchPhaseContext context);

    void onCanMatchPhaseFailure(SearchPhaseContext context);

    void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime);

    void onQueryPhaseStart(SearchPhaseContext context);

    void onQueryPhaseFailure(SearchPhaseContext context);

    void onQueryPhaseEnd(SearchPhaseContext context, long tookTime);

    void onFetchPhaseStart(SearchPhaseContext context);

    void onFetchPhaseFailure(SearchPhaseContext context);

    void onFetchPhaseEnd(SearchPhaseContext context, long tookTime);

    void onExpandSearchPhaseStart(SearchPhaseContext context);

    void onExpandSearchPhaseFailure(SearchPhaseContext context);

    void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime);

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
        public void onDFSPreQueryPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onDFSPreQueryPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onDFSPreQueryPhaseEnd(context, tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onDFSPreQueryPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onDFSPreQueryPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onCanMatchPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onCanMatchPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onCanMatchPhaseEnd(context, tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onCanMatchPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onCanMatchPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onQueryPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onQueryPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onQueryPhaseEnd(context, tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onQueryPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onQueryPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFetchPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onFetchPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onFetchPhaseEnd(context, tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFetchPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onFetchPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchFailure listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onExpandSearchPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onExpandSearchPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchStart listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onExpandSearchPhaseEnd(context, tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchEnd listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onExpandSearchPhaseFailure(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchFailure listener [{}] failed", listener), e);
                }
            }
        }
    }
}
