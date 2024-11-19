/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * An iterator over stream.
 */
@ExperimentalApi
public interface StreamIterator extends Closeable {

    /**
     * Blocking request to load next batch into root.
     *
     * @return true if more data was found, false if the stream is exhausted
     */
    boolean next();

    /**
     * Returns the VectorSchemaRoot associated with this iterator.
     * The content of this root is updated with each successful call to next().
     *
     * @return the VectorSchemaRoot
     */
    VectorSchemaRoot getRoot();
}
