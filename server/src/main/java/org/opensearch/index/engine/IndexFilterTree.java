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
 * </p>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterTree implements Closeable {

    // TODO
    @Override
    public void close() throws IOException {}
}
