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
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterTree implements Closeable {

    // TODO
    @Override
    public void close() throws IOException {}
}
