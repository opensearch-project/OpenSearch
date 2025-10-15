/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        int parallelism = 9; // Adjust as needed for your test
        return List.of(new ForkJoinPoolExecutorBuilder("jvector", parallelism));
    }
}
