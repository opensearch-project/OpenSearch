/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;

import java.util.Collections;
import java.util.List;

/**
 * A listener that is executed on per-index and per-shard store events, like deleting shard path
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public interface IndexStoreListener {
    default void beforeShardPathDeleted(ShardId shardId, IndexSettings indexSettings, NodeEnvironment env) {}

    default void beforeIndexPathDeleted(Index index, IndexSettings indexSettings, NodeEnvironment env) {}

    IndexStoreListener EMPTY = new IndexStoreListener() {
    };

    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.19.0")
    final class CompositeIndexStoreListener implements IndexStoreListener {
        private final List<IndexStoreListener> listeners;
        private final static Logger logger = LogManager.getLogger(CompositeIndexStoreListener.class);

        public CompositeIndexStoreListener(List<IndexStoreListener> listeners) {
            this.listeners = Collections.unmodifiableList(listeners);
        }

        @Override
        public void beforeShardPathDeleted(ShardId shardId, IndexSettings indexSettings, NodeEnvironment env) {
            for (IndexStoreListener listener : listeners) {
                try {
                    listener.beforeShardPathDeleted(shardId, indexSettings, env);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("beforeShardPathDeleted listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void beforeIndexPathDeleted(Index index, IndexSettings indexSettings, NodeEnvironment env) {
            for (IndexStoreListener listener : listeners) {
                try {
                    listener.beforeIndexPathDeleted(index, indexSettings, env);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("beforeIndexPathDeleted listener [{}] failed", listener), e);
                }
            }
        }
    }
}
