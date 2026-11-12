/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * The documents of one leaf a caller wants read in one batch.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BatchRequest {

    private final int[] docs;
    private final BooleanSupplier isCancelled;

    /**
     * @param docs        leaf-relative document ids, strictly ascending; not copied, so the caller must not change them
     * @param isCancelled answers {@code true} once the read should stop
     */
    public BatchRequest(int[] docs, BooleanSupplier isCancelled) {
        this.docs = Objects.requireNonNull(docs);
        this.isCancelled = Objects.requireNonNull(isCancelled);
        if (docs.length == 0) {
            throw new IllegalArgumentException("a batch request needs at least one document");
        }
        for (int i = 1; i < docs.length; i++) {
            if (docs[i] <= docs[i - 1]) {
                throw new IllegalArgumentException("batch documents must be strictly ascending, got " + docs[i - 1] + " then " + docs[i]);
            }
        }
    }

    /** Leaf-relative document ids, strictly ascending. Callers must not modify the array. */
    public int[] docs() {
        return docs;
    }

    /** Whether the read should stop. */
    public boolean isCancelled() {
        return isCancelled.getAsBoolean();
    }
}
