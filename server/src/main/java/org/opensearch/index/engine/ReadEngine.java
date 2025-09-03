/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

/**
 * TODO : will use read engine eventually if we need more functionalities other than SearcherOperations
 */
public abstract class ReadEngine implements SearcherOperations<EngineSearcher, EngineReaderManager<?>> {
  // No-OP
}
