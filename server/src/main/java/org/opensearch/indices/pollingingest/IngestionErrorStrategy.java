/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Locale;

/**
 * Defines the error handling strategy when an error is encountered either during polling records from ingestion source
 * or during processing the polled records.
 */
@ExperimentalApi
public interface IngestionErrorStrategy {

    /**
     * Process and record the error.
     */
    void handleError(Throwable e, ErrorStage stage);

    /**
     * Indicates if the error should be ignored.
     */
    boolean shouldIgnoreError(Throwable e, ErrorStage stage);

    static IngestionErrorStrategy create(ErrorStrategy errorStrategy, String ingestionSource) {
        switch (errorStrategy) {
            case BLOCK:
                return new BlockIngestionErrorStrategy(ingestionSource);
            case DROP:
            default:
                return new DropIngestionErrorStrategy(ingestionSource);
        }
    }

    /**
     * Indicates available error handling strategies
     */
    @ExperimentalApi
    enum ErrorStrategy {
        DROP,
        BLOCK;

        public static ErrorStrategy parseFromString(String errorStrategy) {
            try {
                return ErrorStrategy.valueOf(errorStrategy.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid ingestion errorStrategy: " + errorStrategy, e);
            }
        }
    }

    /**
     * Indicates different stages of encountered errors
     */
    @ExperimentalApi
    enum ErrorStage {
        POLLING,
        PROCESSING
    }

}
