/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.RestoreListenerPlugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tells every {@link RestoreListenerPlugin} that a snapshot restore finished. {@link RestoreService}'s cleanup task
 * calls {@link #restoreCompleted} once the cluster state that removes the finished restore is published, so each
 * restore is reported once. Each plugin runs in its own task on the generic thread pool, off the cluster-state thread,
 * so a slow plugin delays neither cluster-state updates nor the other plugins. A plugin that throws is logged and does
 * not affect the others.
 *
 * @opensearch.internal
 */
final class RestoreCompletionNotifier {

    private static final Logger logger = LogManager.getLogger(RestoreCompletionNotifier.class);

    private final ThreadPool threadPool;

    private final List<RestoreListenerPlugin> plugins;

    RestoreCompletionNotifier(ThreadPool threadPool, List<RestoreListenerPlugin> plugins) {
        this.threadPool = threadPool;
        this.plugins = List.copyOf(plugins);
    }

    /**
     * Calls {@link RestoreListenerPlugin#onRestore} on every plugin, unless no index of the restore had every shard
     * restored.
     */
    void restoreCompleted(RestoreInProgress.Entry entry) {
        if (plugins.isEmpty()) {
            return;
        }
        final RestoreContext context = contextOf(entry);
        if (context == null) {
            logger.debug(
                "restore [{}] of snapshot [{}] restored no index completely, not notifying plugins",
                entry.uuid(),
                entry.snapshot()
            );
            return;
        }
        // This runs on the cluster manager service thread. Plugins must not block that thread, so each call is forked.
        for (RestoreListenerPlugin plugin : plugins) {
            threadPool.generic().execute(() -> callPlugin(plugin, context));
        }
    }

    private void callPlugin(RestoreListenerPlugin plugin, RestoreContext context) {
        try {
            plugin.onRestore(context);
        } catch (Exception e) {
            logger.error(
                () -> new ParameterizedMessage(
                    "[{}] failed to handle restore [{}] of snapshot [{}]",
                    plugin.getClass().getName(),
                    context.restoreUUID(),
                    context.snapshot()
                ),
                e
            );
        }
    }

    /**
     * Returns the context for a finished restore, or {@code null} when no index had every shard restored.
     */
    static RestoreContext contextOf(RestoreInProgress.Entry entry) {
        final Set<Index> indices = new HashSet<>();
        final Set<Index> notRestored = new HashSet<>();
        for (Map.Entry<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards().entrySet()) {
            final Index index = shard.getKey().getIndex();
            indices.add(index);
            if (shard.getValue().state() != RestoreInProgress.State.SUCCESS) {
                notRestored.add(index);
            }
        }
        indices.removeAll(notRestored);
        if (indices.isEmpty()) {
            return null;
        }
        final List<Index> restored = indices.stream().sorted(Comparator.comparing(Index::getName)).toList();
        return new RestoreContext(entry.snapshot(), entry.uuid(), restored, notRestored.isEmpty() == false);
    }
}
