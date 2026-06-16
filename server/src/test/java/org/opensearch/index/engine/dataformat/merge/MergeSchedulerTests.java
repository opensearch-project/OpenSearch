/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the tiering freeze semantics of {@link MergeScheduler}: when frozen, no new merges
 * may be triggered or force-merged, and the frozen state is derived from both an explicit
 * {@link MergeScheduler#freeze()} and the index's {@code INDEX_TIERING_STATE} setting.
 */
public class MergeSchedulerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private IndexSettings indexSettings(IndexModule.TieringState tieringState) {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexModule.INDEX_TIERING_STATE.getKey(), tieringState.name())
                .build()
        );
    }

    private MergeScheduler newScheduler(MergeHandler mergeHandler, IndexModule.TieringState tieringState) {
        return new MergeScheduler(mergeHandler, (result, merge) -> {}, () -> {}, shardId, indexSettings(tieringState), threadPool);
    }

    public void testFreezeBlocksTriggerMerges() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        assertTrue("scheduler must report frozen after freeze()", scheduler.isFrozen());

        scheduler.triggerMerges();
        // Frozen: the scheduler must not even ask the handler to find/register merges.
        verify(mergeHandler, never()).findAndRegisterMerges();
    }

    public void testTriggerMergesWhenNotFrozenInvokesHandler() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        assertFalse("scheduler must not be frozen for a HOT index", scheduler.isFrozen());
        scheduler.triggerMerges();
        verify(mergeHandler, times(1)).findAndRegisterMerges();
    }

    public void testUnfreezeAllowsMergesAgain() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        scheduler.triggerMerges();
        verify(mergeHandler, never()).findAndRegisterMerges();

        scheduler.unfreeze();
        assertFalse("scheduler must report unfrozen after unfreeze()", scheduler.isFrozen());
        scheduler.triggerMerges();
        verify(mergeHandler, times(1)).findAndRegisterMerges();
    }

    public void testIsFrozenReflectsHotToWarmTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT_TO_WARM);
        assertTrue("HOT_TO_WARM tiering state must report frozen", scheduler.isFrozen());
    }

    public void testIsNotFrozenForHotTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);
        assertFalse("HOT tiering state must not report frozen", scheduler.isFrozen());
    }
}
