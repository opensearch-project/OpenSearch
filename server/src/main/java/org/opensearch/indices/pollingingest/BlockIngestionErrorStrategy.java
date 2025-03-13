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
 * This error handling strategy blocks on failures preventing processing of remaining updates in the ingestion source.
 */
public class BlockIngestionErrorStrategy implements IngestionErrorStrategy {
    private static final Logger logger = LogManager.getLogger(BlockIngestionErrorStrategy.class);
    private final String ingestionSource;

    public BlockIngestionErrorStrategy(String ingestionSource) {
        this.ingestionSource = ingestionSource;
    }

    @Override
    public void handleError(Throwable e, ErrorStage stage) {
        logger.error("Error processing update from {}: {}", ingestionSource, e);

        // todo: record blocking update and emit metrics
    }

    @Override
    public boolean shouldPauseIngestion(Throwable e, ErrorStage stage) {
        return true;
    }
}
