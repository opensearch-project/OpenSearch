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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RestoreListenerPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class RestoreCompletionNotifierTests extends OpenSearchTestCase {

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

    private RestoreCompletionNotifier notifier(List<RestoreListenerPlugin> plugins) {
        return new RestoreCompletionNotifier(taskQueue.getThreadPool(), plugins);
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
