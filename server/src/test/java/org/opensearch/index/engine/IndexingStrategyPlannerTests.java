/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class IndexingStrategyPlannerTests extends EngineTestCase {

    public void testPlanOperationAsPrimaryWithNewDocumentProcessedNormally() throws IOException {
        IndexingStrategyPlanner planner = constructDefaultIndexingStrategyPlanner();

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid(doc), primaryTerm.get(), doc);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processNormally(true, 1, 1);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryWithExistingDocumentProcessedNormally() throws IOException {
        VersionValue versionValue = new IndexVersionValue(null, randomLong(), 5L, 1L);
        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(versionValue);

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid(doc), primaryTerm.get(), doc);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processNormally(false, versionValue.version + 1, 1);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryOptimizedAppendOnly() throws IOException {
        IndexingStrategyPlanner planner = constructDefaultIndexingStrategyPlanner();

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.optimizedAppendOnly(1L, 1);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryFailingWithTooManyDocs() throws IOException {
        Exception tooManyDocsException = new RuntimeException("too many docs");
        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(tooManyDocsException);

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(
            doc,
            UNASSIGNED_SEQ_NO,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            randomLongBetween(1, Long.MAX_VALUE),
            0
        );
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.failAsTooManyDocs(tooManyDocsException);

        assertEquals(expectedStrategy, plannedStrategy);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictDocumentNotFound() throws IOException {
        IndexingStrategyPlanner planner = constructDefaultIndexingStrategyPlanner();

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc, 5L, 1L, VersionType.INTERNAL, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, 1L);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.useUpdateDocument);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertTrue(plannedStrategy.currentNotFoundOrDeleted);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictSeqNoMismatch() throws IOException {
        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(new IndexVersionValue(null, 2L, 10L, 2L));

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc, 5L, 1L, VersionType.INTERNAL, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, 1L);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.useUpdateDocument);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertFalse(plannedStrategy.currentNotFoundOrDeleted);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryVersionConflictForWrites() throws IOException {
        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(new IndexVersionValue(null, 5L, 10L, 1L));

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc, UNASSIGNED_SEQ_NO, 3L, VersionType.EXTERNAL, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, 0L);
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.useUpdateDocument);
        assertFalse(plannedStrategy.addStaleOpToEngine);
        assertEquals(Versions.NOT_FOUND, plannedStrategy.version);
        assertFalse(plannedStrategy.currentNotFoundOrDeleted);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsPrimaryAppendOnlyIndexRetry() throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), "true");
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        EngineConfig engineConfig = config(indexSettings, store, createTempDir(), newMergePolicy(), null);

        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(engineConfig, new IndexVersionValue(null, 5L, 10L, 1L));

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(
            doc,
            UNASSIGNED_SEQ_NO,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            0L
        );
        IndexingStrategy plannedStrategy = planner.planOperationAsPrimary(index);

        assertFalse(plannedStrategy.executeOpOnEngine);
        assertFalse(plannedStrategy.useUpdateDocument);
        assertTrue(plannedStrategy.addStaleOpToEngine);
        assertEquals(5L, plannedStrategy.version);
        assertFalse(plannedStrategy.currentNotFoundOrDeleted);
        assertEquals(0, plannedStrategy.reservedDocs);
        assertTrue(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessedBefore() throws IOException {
        IndexingStrategyPlanner planner = constructIndexingStrategyPlanner(
            operation -> true,
            (operation, aBoolean) -> null,
            (operation, integer) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 10L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processButSkipEngine(false, index.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryOptimizedAppendOnly() throws IOException {
        IndexingStrategyPlanner planner = new IndexingStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 5L,
            () -> 10L,
            op -> false,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            (op, refresh) -> null,
            (timestamp, append) -> {},
            (op, docs) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 15L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.optimizedAppendOnly(index.version(), 0);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessAsStaleOp() throws IOException {
        IndexingStrategyPlanner planner = new IndexingStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 15L,
            () -> 10L,
            op -> false,
            op -> OpVsEngineDocStatus.OP_STALE_OR_EQUAL,
            (op, refresh) -> null,
            (timestamp, append) -> {},
            (op, docs) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 12L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processAsStaleOp(index.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessNormally() throws IOException {
        IndexingStrategyPlanner planner = new IndexingStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 15L,
            () -> 10L,
            op -> false,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            (op, refresh) -> null,
            (timestamp, append) -> {},
            (op, docs) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 12L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processNormally(true, index.version(), 0);

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    public void testPlanOperationAsNonPrimaryProcessNormallyDocumentExists() throws IOException {
        IndexingStrategyPlanner planner = new IndexingStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 15L,
            () -> 10L,
            op -> false,
            op -> OpVsEngineDocStatus.OP_NEWER,
            (op, refresh) -> null,
            (timestamp, append) -> {},
            (op, docs) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 12L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processNormally(false, index.version(), 0);

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

        IndexingStrategyPlanner planner = new IndexingStrategyPlanner(
            engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 15L,
            () -> 10L,
            op -> false,
            op -> OpVsEngineDocStatus.OP_STALE_OR_EQUAL,
            (op, refresh) -> null,
            (timestamp, append) -> {},
            (op, docs) -> null
        );

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = replicaIndexForDoc(doc, 1L, 12L, false);
        IndexingStrategy plannedStrategy = planner.planOperationAsNonPrimary(index);
        IndexingStrategy expectedStrategy = IndexingStrategy.processButSkipEngine(false, index.version());

        assertEquals(expectedStrategy, plannedStrategy);
        assertFalse(plannedStrategy.earlyResultOnPreFlightError.isPresent());
    }

    private Engine.Index indexForDoc(
        ParsedDocument doc,
        long seqNo,
        long version,
        VersionType versionType,
        long autoGeneratedIdTimestamp,
        long ifPrimaryTerm
    ) {
        return new Engine.Index(
            newUid(doc),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            versionType,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            false,
            seqNo,
            ifPrimaryTerm
        );
    }

    private IndexingStrategyPlanner constructDefaultIndexingStrategyPlanner() {
        return constructIndexingStrategyPlanner(operation -> false, (operation, aBoolean) -> null, (op, docs) -> null);
    }

    private IndexingStrategyPlanner constructIndexingStrategyPlanner(VersionValue versionValue) {
        return constructIndexingStrategyPlanner(operation -> false, (operation, aBoolean) -> versionValue, (op, docs) -> null);
    }

    private IndexingStrategyPlanner constructIndexingStrategyPlanner(Exception e) {
        return constructIndexingStrategyPlanner(operation -> false, (operation, aBoolean) -> null, (op, docs) -> e);
    }

    private IndexingStrategyPlanner constructIndexingStrategyPlanner(EngineConfig engineConfig, VersionValue versionValue) {
        return new IndexingStrategyPlanner(
            engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 0L,
            () -> 0L,
            operation -> false,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            (operation, aBoolean) -> versionValue,
            (timestamp, append) -> {},
            (op, docs) -> null
        );
    }

    private IndexingStrategyPlanner constructIndexingStrategyPlanner(
        Predicate<Engine.Operation> hasBeenProcessedBefore,
        CheckedBiFunction<Engine.Operation, Boolean, VersionValue, IOException> docVersionSupplier,
        BiFunction<Engine.Operation, Integer, Exception> tryAcquireInFlightDocs
    ) {
        return new IndexingStrategyPlanner(
            engine.engineConfig.getIndexSettings(),
            engine.shardId,
            engine.versionMap,
            () -> 0L,
            () -> 0L,
            () -> 0L,
            hasBeenProcessedBefore,
            op -> OpVsEngineDocStatus.DOC_NOT_FOUND,
            docVersionSupplier,
            (timestamp, append) -> {},
            tryAcquireInFlightDocs
        );
    }
}
