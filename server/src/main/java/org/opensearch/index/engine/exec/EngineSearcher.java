/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.SearchExecutionContext;

import java.io.IOException;

/**
 * Engine-agnostic searcher interface for pluggable backends.
 *
 * @param <C> the context type this searcher operates on
 * @opensearch.experimental
 */
@ExperimentalApi
public interface EngineSearcher<C extends SearchExecutionContext> extends Releasable {

    void search(C context) throws IOException;
}
