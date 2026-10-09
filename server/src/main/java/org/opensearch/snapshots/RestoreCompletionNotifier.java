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
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.PluginSubject;
import org.opensearch.plugins.Plugin;
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
 * so a slow plugin delays neither cluster-state updates nor the other plugins. Each task runs as that plugin's own
 * {@link PluginSubject}. A plugin that throws is logged and does not affect the others.
 *
 * @opensearch.internal
 */
final class RestoreCompletionNotifier {

    private static final Logger logger = LogManager.getLogger(RestoreCompletionNotifier.class);

    private final ThreadPool threadPool;

    private final List<Listener> listeners;

    /**
     * @param identityService holds the subject core assigned to each plugin; every plugin must have one
     */
    RestoreCompletionNotifier(ThreadPool threadPool, List<RestoreListenerPlugin> plugins, IdentityService identityService) {
        this.threadPool = threadPool;
        this.listeners = plugins.stream().map(plugin -> {
            final PluginSubject subject = identityService.getPluginSubject((Plugin) plugin);
            if (subject == null) {
                throw new IllegalStateException("restore listener plugin [" + plugin.getClass().getName() + "] has no plugin subject");
            }
            return new Listener(plugin, subject);
        }).toList();
    }

    /**
     * Calls {@link RestoreListenerPlugin#onRestore} on every plugin, unless no index of the restore had every shard
     * restored.
     */
    void restoreCompleted(RestoreInProgress.Entry entry) {
        if (listeners.isEmpty()) {
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
        // This runs on the cluster manager service thread in system context. Plugins must not block that thread, so each
        // call is forked. runAs replaces the inherited system context with the plugin's own identity.
        for (Listener listener : listeners) {
            threadPool.generic().execute(() -> callPlugin(listener, context));
        }
    }

    private static void callPlugin(Listener listener, RestoreContext context) {
        final RestoreListenerPlugin plugin = listener.plugin();
        try {
            listener.subject().runAs(() -> plugin.onRestore(context));
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

    private record Listener(RestoreListenerPlugin plugin, PluginSubject subject) {
    }
}
