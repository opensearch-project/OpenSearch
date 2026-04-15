/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provider interface for Substrait-driven tree query evaluation.
 * <p>
 * Any index engine (Lucene, Tantivy, etc.) implements this to participate
 * in runtime collector creation during boolean tree query execution.
 * The lifecycle is:
 * <ol>
 *   <li>{@link #createProvider(byte[])} — create a provider instance from serialized query bytes</li>
 *   <li>{@link #createCollector(int, int, int, int)} — create a per-segment collector</li>
 *   <li>{@link #collectDocs(int, int, int)} — collect matching doc IDs as a packed bitset</li>
 *   <li>{@link #releaseCollector(int)} — release per-segment collector resources</li>
 *   <li>{@link #releaseProvider(int)} — release provider and all remaining collectors</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterCollectorProvider extends Closeable {

    /**
     * Creates a provider instance for the given query.
     * Called once per unique (column, value) pair in the tree.
     *
     * @param queryBytes serialized query (e.g., Lucene query bytes)
     * @return providerKey identifying this provider instance
     * @throws IOException if provider creation fails
     */
    int createProvider(byte[] queryBytes) throws IOException;

    /**
     * Creates a per-segment collector for the given provider.
     *
     * @param providerKey from {@link #createProvider(byte[])}
     * @param segmentOrd  segment ordinal
     * @param minDoc      inclusive lower bound
     * @param maxDoc      exclusive upper bound
     * @return collectorKey for subsequent collect/release calls
     */
    int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc);

    /**
     * Collects matching doc IDs as a packed bitset.
     *
     * @param collectorKey from {@link #createCollector(int, int, int, int)}
     * @param minDoc       inclusive lower bound
     * @param maxDoc       exclusive upper bound
     * @return packed long[] bitset (LSB-first, bit i of word[j] = doc minDoc + j*64 + i)
     */
    long[] collectDocs(int collectorKey, int minDoc, int maxDoc);

    /**
     * Releases a per-segment collector.
     *
     * @param collectorKey the collector key from {@link #createCollector(int, int, int, int)}
     */
    void releaseCollector(int collectorKey);

    /**
     * Releases a provider instance and all its remaining collectors.
     *
     * @param providerKey the provider key from {@link #createProvider(byte[])}
     */
    void releaseProvider(int providerKey);
}
