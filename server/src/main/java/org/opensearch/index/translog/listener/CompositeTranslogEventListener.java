/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.TranslogException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The listener that multiplexes other {@link TranslogEventListener}
 *
 * @opensearch.internal
 */
public final class CompositeTranslogEventListener implements TranslogEventListener {

    private final List<TranslogEventListener> listeners;
    private final ShardId shardId;
    private final Logger logger = LogManager.getLogger(CompositeTranslogEventListener.class);

    public CompositeTranslogEventListener(Collection<TranslogEventListener> listeners, ShardId shardId) {
        this.shardId = shardId;
        for (TranslogEventListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listeners must be non-null");
            }
        }
        this.listeners = Collections.unmodifiableList(new ArrayList<>(listeners));
    }

    @Override
    public void onAfterTranslogSync() {
        List<Exception> exceptionList = new ArrayList<>(listeners.size());
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onAfterTranslogSync();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onTranslogSync listener"), ex);
                exceptionList.add(ex);
            }
        }
        maybeThrowTranslogExceptionAndSuppress(exceptionList);
    }

    @Override
    public void onAfterTranslogRecovery() {
        List<Exception> exceptionList = new ArrayList<>(listeners.size());
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onAfterTranslogRecovery();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onTranslogRecovery listener"), ex);
                exceptionList.add(ex);
            }
        }
        maybeThrowTranslogExceptionAndSuppress(exceptionList);
    }

    @Override
    public void onBeginTranslogRecovery() {
        List<Exception> exceptionList = new ArrayList<>(listeners.size());
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onBeginTranslogRecovery();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onBeginTranslogRecovery listener"), ex);
                exceptionList.add(ex);
            }
        }
        maybeThrowTranslogExceptionAndSuppress(exceptionList);
    }

    @Override
    public void onFailure(String reason, Exception e) {
        List<Exception> exceptionList = new ArrayList<>(listeners.size());
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onFailure(reason, e);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onFailure listener"), ex);
                exceptionList.add(ex);
            }
        }
        maybeThrowTranslogExceptionAndSuppress(exceptionList);
    }

    private <T extends Throwable> void maybeThrowTranslogExceptionAndSuppress(List<T> exceptions) {
        T main = null;
        for (T ex : exceptions) {
            main = ExceptionsHelper.useOrSuppress(main, ex);
        }
        if (main != null) {
            throw new TranslogException(shardId, "Error while executing translog event listener", main);
        }
    }
}
