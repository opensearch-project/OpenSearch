/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class DeletionStrategyPlannerTests extends EngineTestCase {

    public void testPlanOperationAsPrimary() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlanner(new DeleteVersionValue(1L, 5L, 1L, 1L));

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            0L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processNormally(true, 2, 1);

        assertEquals(expectedStrategy, plannedStrategy);
    }

    public void testPlanOperationAsPrimaryFailAsTooManyDocs() throws IOException {
        Exception reserveError = new IllegalStateException("Too many documents");
        DeletionStrategyPlanner planner = new DeletionStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            op -> false,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            (op, refresh) -> new DeleteVersionValue(1L, 5L, 1L, 1L),
            (op, docs) -> reserveError,
            () -> true
        );

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            0L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.failAsTooManyDocs(reserveError);

        assertEquals(expectedStrategy, plannedStrategy);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflict() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlanner(new IndexVersionValue(null, 1L, 1L, 1L));

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            2L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            1L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.currentlyDeleted);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictDocumentDeleted() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlanner(new DeleteVersionValue(1L, 5L, 1L, 1L));

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            2L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            1L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertTrue(plannedStrategy.currentlyDeleted);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictSeqNoMismatch() throws IOException {
        VersionValue versionValue = new IndexVersionValue(null, 2L, 10L, 2L);
        DeletionStrategyPlanner planner = constructDeletionStrategyPlanner(versionValue);

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            5L,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            5L,
            1L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.currentlyDeleted);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictForWrites() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlanner(new DeleteVersionValue(1L, 5L, 1L, 1L));

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            1L,
            primaryTerm.get(),
            3L,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            1L,
            0L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsPrimary(delete);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertTrue(plannedStrategy.currentlyDeleted);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessedBefore() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlannerForNonPrimary(true, OpVsEngineDocStatus.DOC_NOT_FOUND);

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            10L,
            primaryTerm.get(),
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            0L
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsNonPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processButSkipEngine(false, delete.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessAsStaleOp() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlannerForNonPrimary(false, OpVsEngineDocStatus.OP_STALE_OR_EQUAL);

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsNonPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processAsStaleOp(delete.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessNormally() throws IOException {
        DeletionStrategyPlanner planner = constructDeletionStrategyPlannerForNonPrimary(false, OpVsEngineDocStatus.DOC_NOT_FOUND);

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsNonPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processNormally(true, delete.version(), 0);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessNormallyDocumentExists() throws IOException {
        DeletionStrategyPlanner planner = new DeletionStrategyPlanner(
            replicaEngine.engineConfig.getIndexSettings(),
            replicaEngine.shardId,
            op -> false,
            op -> OpVsEngineDocStatus.OP_NEWER,
            (op, refresh) -> null,
            (op, docs) -> null,
            () -> true
        );

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsNonPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processNormally(false, delete.version(), 0);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryWithSegRepEnabled() throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT");
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        EngineConfig engineConfig = config(indexSettings, store, createTempDir(), newMergePolicy(), null);

        DeletionStrategyPlanner planner = new DeletionStrategyPlanner(
            engineConfig.getIndexSettings(),
            replicaEngine.shardId,
            op -> false,
            op -> OpVsEngineDocStatus.OP_STALE_OR_EQUAL,
            (op, refresh) -> null,
            (op, docs) -> null,
            () -> true
        );

        Engine.Delete delete = new Engine.Delete(
            "1",
            newUid("1"),
            10L,
            primaryTerm.get(),
            UNASSIGNED_SEQ_NO,
            null,
            Engine.Operation.Origin.REPLICA,
            System.currentTimeMillis(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );
        DeletionStrategy plannedStrategy = planner.planOperationAsNonPrimary(delete);
        DeletionStrategy expectedStrategy = DeletionStrategy.processButSkipEngine(false, delete.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    private DeletionStrategyPlanner constructDeletionStrategyPlanner(VersionValue versionValue) {
        return new DeletionStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            op -> false,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            (op, refresh) -> versionValue,
            (op, docs) -> null,
            () -> true
        );
    }

    private DeletionStrategyPlanner constructDeletionStrategyPlannerForNonPrimary(boolean processedBefore, OpVsEngineDocStatus docStatus) {
        return new DeletionStrategyPlanner(
            replicaEngine.engineConfig.getIndexSettings(),
            replicaEngine.shardId,
            op -> processedBefore,
            op -> docStatus,
            (op, refresh) -> null,
            (op, docs) -> null,
            () -> true
        );
    }
}
