/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RestoreListenerPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class RestoreCompletionNotifierTests extends OpenSearchTestCase {

    private static final String SUBJECT = "test_subject";

    private DeterministicTaskQueue taskQueue;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
    }

    public void testContextListsOnlyIndicesWhoseEveryShardWasRestored() {
        final Index restored = new Index("restored", "restored-uuid");
        final Index failed = new Index("failed", "failed-uuid");
        final RestoreInProgress.Entry entry = entry(
            "restore-uuid",
            Map.of(
                new ShardId(restored, 0),
                RestoreInProgress.State.SUCCESS,
                new ShardId(restored, 1),
                RestoreInProgress.State.SUCCESS,
                new ShardId(failed, 0),
                RestoreInProgress.State.SUCCESS,
                new ShardId(failed, 1),
                RestoreInProgress.State.FAILURE
            )
        );

        final RestoreContext context = RestoreCompletionNotifier.contextOf(entry);

        assertEquals(entry.snapshot(), context.snapshot());
        assertEquals("restore-uuid", context.restoreUUID());
        assertEquals(List.of(restored), context.restoredIndices());
        assertTrue(context.partialFailure());
    }

    public void testContextListsIndicesByName() {
        final Index first = new Index("a-index", "uuid-2");
        final Index second = new Index("b-index", "uuid-1");
        final RestoreInProgress.Entry entry = entry(
            "restore-uuid",
            Map.of(new ShardId(second, 0), RestoreInProgress.State.SUCCESS, new ShardId(first, 0), RestoreInProgress.State.SUCCESS)
        );

        final RestoreContext context = RestoreCompletionNotifier.contextOf(entry);

        assertEquals(List.of(first, second), context.restoredIndices());
        assertFalse(context.partialFailure());
    }

    public void testRestoreWithNoFullyRestoredIndexIsNotReported() {
        final RestoreInProgress.Entry entry = entry(
            "restore-uuid",
            Map.of(new ShardId(new Index("failed", "failed-uuid"), 0), RestoreInProgress.State.FAILURE)
        );
        assertNull(RestoreCompletionNotifier.contextOf(entry));

        final RecordingPlugin plugin = new RecordingPlugin(context -> {});
        notifier(List.of(plugin)).restoreCompleted(entry);

        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(plugin.calls.isEmpty());
    }

    public void testNoPluginsIsANoOp() {
        notifier(List.of()).restoreCompleted(restoredEntry("restore-uuid"));
        assertFalse(taskQueue.hasRunnableTasks());
    }

    public void testEveryPluginIsCalledOnTheThreadPoolNotTheCallingThread() {
        final RecordingPlugin first = new RecordingPlugin(context -> {});
        final RecordingPlugin second = new RecordingPlugin(context -> {});
        final RestoreCompletionNotifier notifier = notifier(List.of(first, second));

        notifier.restoreCompleted(restoredEntry("restore-uuid"));
        assertTrue("nothing runs on the cluster manager thread", first.calls.isEmpty() && second.calls.isEmpty());
        taskQueue.runAllTasks();

        assertEquals(List.of("restore-uuid"), first.restoreUUIDs());
        assertEquals(List.of("restore-uuid"), second.restoreUUIDs());
    }

    public void testFailingPluginDoesNotStopTheOthers() {
        final RecordingPlugin failing = new RecordingPlugin(context -> { throw new RuntimeException("simulated"); });
        final RecordingPlugin other = new RecordingPlugin(context -> {});
        final RestoreCompletionNotifier notifier = notifier(List.of(failing, other));

        notifier.restoreCompleted(restoredEntry("restore-uuid"));
        taskQueue.runAllTasks();

        assertEquals("a failed call is not retried", List.of("restore-uuid"), failing.restoreUUIDs());
        assertEquals(List.of("restore-uuid"), other.restoreUUIDs());
    }

    public void testEachPluginRunsAsItsOwnSubject() {
        final ThreadPool threadPool = taskQueue.getThreadPool();
        final ThreadContext threadContext = threadPool.getThreadContext();
        final List<String> seen = new CopyOnWriteArrayList<>();
        final RecordingPlugin first = new RecordingPlugin(context -> seen.add("first ran as " + threadContext.getTransient(SUBJECT)));
        final RecordingPlugin second = new RecordingPlugin(context -> seen.add("second ran as " + threadContext.getTransient(SUBJECT)));
        final Map<Plugin, String> names = Map.of(first, "first", second, "second");
        final List<RestoreListenerPlugin> plugins = List.of(first, second);

        new RestoreCompletionNotifier(threadPool, plugins, identityService(threadPool, plugins, names::get)).restoreCompleted(
            restoredEntry("restore-uuid")
        );
        taskQueue.runAllTasks();

        assertEquals(Set.of("first ran as first", "second ran as second"), Set.copyOf(seen));
        assertNull("the subject is not left on the calling thread", threadContext.getTransient(SUBJECT));
    }

    public void testPluginWithoutASubjectIsRejected() {
        final ThreadPool threadPool = taskQueue.getThreadPool();
        final RecordingPlugin plugin = new RecordingPlugin(context -> {});
        // The identity service never assigned this plugin a subject.
        final IdentityService identityService = identityService(threadPool, List.of(), p -> "unused");
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new RestoreCompletionNotifier(threadPool, List.of(plugin), identityService)
        );
        assertEquals("restore listener plugin [" + RecordingPlugin.class.getName() + "] has no plugin subject", e.getMessage());
    }

    public void testPluginDoesNotInheritTheCallersContextWithCoreSubject() throws Exception {
        // A real thread pool, because it carries the submitting thread's context into the task, and the subject that core
        // assigns when no identity plugin is installed.
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final CompletableFuture<Optional<String>> headerSeen = new CompletableFuture<>();
            final CompletableFuture<Boolean> systemContextSeen = new CompletableFuture<>();
            final RecordingPlugin plugin = new RecordingPlugin(context -> {
                headerSeen.complete(Optional.ofNullable(threadContext.getHeader("caller-header")));
                systemContextSeen.complete(threadContext.isSystemContext());
            });
            final IdentityService identityService = new IdentityService(Settings.EMPTY, threadPool, List.of());
            identityService.initializeIdentityAwarePlugins(List.of(plugin));
            final RestoreCompletionNotifier notifier = new RestoreCompletionNotifier(threadPool, List.of(plugin), identityService);

            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("caller-header", "from the cluster manager thread");
                threadContext.markAsSystemContext();
                notifier.restoreCompleted(restoredEntry("restore-uuid"));
            }

            assertEquals(Optional.empty(), headerSeen.get(10, TimeUnit.SECONDS));
            assertFalse(systemContextSeen.get(10, TimeUnit.SECONDS));
        } finally {
            terminate(threadPool);
        }
    }

    private RestoreCompletionNotifier notifier(List<RestoreListenerPlugin> plugins) {
        final ThreadPool threadPool = taskQueue.getThreadPool();
        return new RestoreCompletionNotifier(threadPool, plugins, identityService(threadPool, plugins, p -> "plugin"));
    }

    /**
     * An identity service whose identity plugin gives each plugin a {@link NamedSubject}, already assigned to the given
     * plugins the way {@code Node} does at startup.
     */
    private static IdentityService identityService(
        ThreadPool threadPool,
        List<RestoreListenerPlugin> plugins,
        Function<Plugin, String> nameOf
    ) {
        final IdentityPlugin namingIdentityPlugin = new IdentityPlugin() {
            @Override
            public Subject getCurrentSubject() {
                return null;
            }

            @Override
            public TokenManager getTokenManager() {
                return null;
            }

            @Override
            public PluginSubject getPluginSubject(Plugin plugin) {
                return new NamedSubject(nameOf.apply(plugin), threadPool.getThreadContext());
            }
        };
        final IdentityService identityService = new IdentityService(Settings.EMPTY, threadPool, List.of(namingIdentityPlugin));
        identityService.initializeIdentityAwarePlugins(new ArrayList<>(plugins));
        return identityService;
    }

    private static RestoreInProgress.Entry restoredEntry(String uuid) {
        return entry(uuid, Map.of(new ShardId(new Index("index", "index-uuid"), 0), RestoreInProgress.State.SUCCESS));
    }

    private static RestoreInProgress.Entry entry(String uuid, Map<ShardId, RestoreInProgress.State> shardStates) {
        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = new HashMap<>();
        shardStates.forEach((shardId, state) -> shards.put(shardId, new RestoreInProgress.ShardRestoreStatus("node", state)));
        final boolean failed = shardStates.containsValue(RestoreInProgress.State.FAILURE);
        return new RestoreInProgress.Entry(
            uuid,
            new Snapshot("repo", new SnapshotId("snapshot", "snapshot-uuid")),
            failed ? RestoreInProgress.State.FAILURE : RestoreInProgress.State.SUCCESS,
            shardStates.keySet().stream().map(ShardId::getIndexName).distinct().toList(),
            shards
        );
    }

    /**
     * Runs code with its name in the thread context, the way a real subject runs code as its plugin.
     */
    private record NamedSubject(String name, ThreadContext threadContext) implements PluginSubject {

        @Override
        public Principal getPrincipal() {
            return new NamedPrincipal(name);
        }

        @Override
        public <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putTransient(SUBJECT, name);
                r.run();
            }
        }
    }

    private static final class RecordingPlugin extends Plugin implements RestoreListenerPlugin {

        private final Consumer<RestoreContext> behaviour;

        private final List<RestoreContext> calls = new CopyOnWriteArrayList<>();

        RecordingPlugin(Consumer<RestoreContext> behaviour) {
            this.behaviour = behaviour;
        }

        @Override
        public void onRestore(RestoreContext context) {
            calls.add(context);
            behaviour.accept(context);
        }

        List<String> restoreUUIDs() {
            return calls.stream().map(RestoreContext::restoreUUID).toList();
        }
    }
}
