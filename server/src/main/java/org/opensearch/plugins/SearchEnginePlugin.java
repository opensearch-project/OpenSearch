/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.SearchExecutionEngine;

import java.io.IOException;

/**
 * Plugin interface for extending OpenSearch engine functionality.
 * This interface allows plugins to extend the core engine capabilities.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface SearchEnginePlugin {
    /**
     * createEngine
     * @return
     * @throws IOException
     */
    SearchExecutionEngine createEngine() throws IOException;
}
