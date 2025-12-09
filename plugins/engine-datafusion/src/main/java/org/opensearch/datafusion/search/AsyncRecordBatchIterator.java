/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.RecordBatchStream;

/**
 * Async iterator over Arrow record batches from a RecordBatchStream using ActionListener.
 */
public class AsyncRecordBatchIterator {

    private final RecordBatchStream stream;
    private Boolean hasNext;

    public AsyncRecordBatchIterator(RecordBatchStream stream) {
        this.stream = stream;
    }

    /**
     * Asynchronously check if there's a next batch available.
     */
    public void nextAsync(ActionListener<Boolean> listener) {
        if (hasNext != null) {
            listener.onResponse(hasNext);
            return;
        }

        stream.loadNextBatch().whenComplete((result, throwable) -> {
            if (throwable != null) {
                listener.onFailure(new RuntimeException("Failed to load next batch", throwable));
            } else {
                hasNext = result;
                listener.onResponse(hasNext);
            }
        });
    }
}
