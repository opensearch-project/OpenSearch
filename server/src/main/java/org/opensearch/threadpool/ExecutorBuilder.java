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

package org.opensearch.threadpool;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutorsUtils;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * Base class for executor builders.
 *
 * @param <U> the underlying type of the executor settings
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ExecutorBuilder<U extends ExecutorBuilder.ExecutorSettings> {

    private final String name;

    public ExecutorBuilder(String name) {
        this.name = name;
    }

    protected String name() {
        return name;
    }

    protected static String settingsKey(final String prefix, final String key) {
        return String.join(".", prefix, key);
    }

    protected int applyHardSizeLimit(final Settings settings, final String name) {
        if (name.equals("bulk") || name.equals(ThreadPool.Names.WRITE) || name.equals(ThreadPool.Names.SYSTEM_WRITE)) {
            return 1 + OpenSearchExecutorsUtils.allocatedProcessors(settings);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * The list of settings this builder will register.
     *
     * @return the list of registered settings
     */
    public abstract List<Setting<?>> getRegisteredSettings();

    /**
     * Return an executor settings object from the node-level settings.
     *
     * @param settings the node-level settings
     * @return the executor settings object
     */
    abstract U getSettings(Settings settings);

    /**
     * Builds the executor with the specified executor settings.
     *
     * @param settings      the executor settings
     * @param threadContext the current thread context
     * @return a new executor built from the specified executor settings
     */
    abstract ThreadPool.ExecutorHolder build(U settings, ThreadContext threadContext);

    /**
     * Format the thread pool info object for this executor.
     *
     * @param info the thread pool info object to format
     * @return a formatted thread pool info (useful for logging)
     */
    abstract String formatInfo(ThreadPool.Info info);

    abstract static class ExecutorSettings {

        protected final String nodeName;

        ExecutorSettings(String nodeName) {
            this.nodeName = nodeName;
        }

    }

}
