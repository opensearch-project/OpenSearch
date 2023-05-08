/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.threadpool;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * Base class for executor builders.
*
* @param <U> the underlying type of the executor settings
*
* @opensearch.internal
*/
public abstract class ProtobufExecutorBuilder<U extends ProtobufExecutorBuilder.ExecutorSettings> {

    private final String name;

    public ProtobufExecutorBuilder(String name) {
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
            return 1 + OpenSearchExecutors.allocatedProcessors(settings);
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
    abstract ProtobufThreadPool.ExecutorHolder build(U settings, ThreadContext threadContext);

    /**
     * Format the thread pool info object for this executor.
    *
    * @param info the thread pool info object to format
    * @return a formatted thread pool info (useful for logging)
    */
    abstract String formatInfo(ProtobufThreadPool.Info info);

    abstract static class ExecutorSettings {

        protected final String nodeName;

        ExecutorSettings(String nodeName) {
            this.nodeName = nodeName;
        }

    }

}
