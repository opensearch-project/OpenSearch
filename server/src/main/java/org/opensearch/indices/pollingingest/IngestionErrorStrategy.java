/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.annotation.PublicApi;

import java.util.Locale;

/**
 * Defines the error handling strategy when an error is encountered either during polling records from ingestion source
 * or during processing the polled records.
 */
@PublicApi(since = "3.6.0")
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
     * Returns the name of the error policy.
     */
    String getName();

    /**
     * Indicates available error handling strategies
     */
    @PublicApi(since = "3.6.0")
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
    @PublicApi(since = "3.6.0")
    enum ErrorStage {
        POLLING,
        PROCESSING
    }

}
