/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This error handling strategy drops failures and proceeds with remaining updates in the ingestion source.
 */
public class DropIngestionErrorStrategy implements IngestionErrorStrategy {
    private static final Logger logger = LogManager.getLogger(DropIngestionErrorStrategy.class);
    private final String ingestionSource;

    public DropIngestionErrorStrategy(String ingestionSource) {
        this.ingestionSource = ingestionSource;
    }

    @Override
    public void handleError(Throwable e, ErrorStage stage) {
        logger.error("Error processing update from {}: {}", ingestionSource, e);

        // todo: record failed update stats and emit metrics
    }

    @Override
    public boolean shouldIgnoreError(Throwable e, ErrorStage stage) {
        return true;
    }

}
