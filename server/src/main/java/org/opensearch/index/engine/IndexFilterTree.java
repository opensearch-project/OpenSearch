/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Boolean tree structure for multi-engine query decomposition.
 * <p>
 * Wraps the root node and provides compact array
 * serialization for JNI transport to the Rust layer.
 * <p>
 * Stub — full implementation in task 11.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterTree implements Closeable {

    // TODO(task-11): root node, serialization, provider collection

    @Override
    public void close() throws IOException {
        // TODO(task-11): close all providers, remove from ProviderRegistry
    }
}
