/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * A leaf reader that can read the values of a known set of its documents in one batched read, instead of one
 * document at a time.
 *
 * <p>The contract has two steps:
 * <ol>
 *   <li>{@link #plan} decides, without reading document data, whether this leaf can serve the request. It returns
 *   {@code null} when it cannot; the caller then reads the leaf one document at a time.</li>
 *   <li>{@link #open} reads a plan's documents. A provider that cannot read them throws, which fails the caller's
 *   request: a {@code CircuitBreakingException} when the batch would use too much memory, another exception when the
 *   read fails.</li>
 * </ol>
 *
 * <p>A caller releases every plan it made, opened or not, and closes every batch it opened, on the thread that
 * opened it.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BatchedDocumentValuesProvider {

    /**
     * Decides whether this leaf can serve {@code request}, without reading document data.
     *
     * @return a plan to {@link #open}, or {@code null} if this leaf cannot serve the request
     */
    @Nullable
    BatchPlan plan(BatchRequest request) throws IOException;

    /**
     * Reads the documents of a plan this provider returned and serves them on the calling thread until closed. Frees
     * whatever it read before throwing.
     */
    BatchedDocumentValues open(BatchPlan plan) throws IOException;
}
