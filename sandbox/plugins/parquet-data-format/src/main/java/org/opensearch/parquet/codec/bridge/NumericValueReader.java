/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import java.io.IOException;

/** Minimal decoded-batch source used by the numeric DocValues iterator. */
public interface NumericValueReader {

    /** The currently decoded batch, or {@code null} when none is loaded. */
    DecodedBatch decodedBatch();

    /** Loads a decoded batch containing {@code row}. */
    void loadBatchContaining(long row) throws IOException;
}
