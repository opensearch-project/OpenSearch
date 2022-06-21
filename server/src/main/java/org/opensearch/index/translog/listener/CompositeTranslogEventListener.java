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
    private final Logger logger = LogManager.getLogger(CompositeTranslogEventListener.class);

    public CompositeTranslogEventListener(Collection<TranslogEventListener> listeners) {
        for (TranslogEventListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listeners must be non-null");
            }
        }
        this.listeners = Collections.unmodifiableList(new ArrayList<>(listeners));
    }

    @Override
    public void onTranslogSync() {
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onTranslogSync();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onTranslogSync listener"), ex);
            }
        }
    }

    @Override
    public void onTranslogRecovery() {
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onTranslogRecovery();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onTranslogRecovery listener"), ex);
            }
        }
    }

    @Override
    public void onBeginTranslogRecovery() {
        for (TranslogEventListener listener : listeners) {
            try {
                listener.onBeginTranslogRecovery();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke onBeginTranslogRecovery listener"), ex);
            }
        }
    }
}
