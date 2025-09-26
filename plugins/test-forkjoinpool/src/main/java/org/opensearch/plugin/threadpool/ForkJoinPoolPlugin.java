/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugin.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ForkJoinPoolExecutorBuilder;

import java.util.List;

/**
 * Plugin to register a ForkJoinPool-based thread pool for testing.
 */
public class ForkJoinPoolPlugin extends Plugin {
    /**
     * Default constructor for ForkJoinPoolPlugin.
     */
    public ForkJoinPoolPlugin() {
        super();
    }

    /**
     * Registers a ForkJoinPool thread pool named "jvector" with parallelism 2.
     *
     * @param settings The OpenSearch settings.
     * @return A list of ExecutorBuilders to be registered.
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int parallelism = 2; // Adjust as needed for your test
        return List.of(new ForkJoinPoolExecutorBuilder("jvector", parallelism));
    }
}
