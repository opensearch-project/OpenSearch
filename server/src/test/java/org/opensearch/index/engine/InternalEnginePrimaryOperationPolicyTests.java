/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.Path;

public class InternalEnginePrimaryOperationPolicyTests extends EngineTestCase {

    private EngineConfig preAssignedSeqNoConfig(Store store, Path translogPath) {
        return config(defaultSettings, store, translogPath, newMergePolicy(), null).toBuilder()
            .primaryOperationPolicy(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE)
            .build();
    }

    /** A primary-origin op carrying an upstream-assigned seq no., as a replication plugin replays. */
    private Engine.Index upstreamIndexForDoc(ParsedDocument doc, long seqNo, long version, long ifSeqNo, long ifPrimaryTerm) {
        return new Engine.Index(
            newUid(doc),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    public void testDefaultProviderIsUsedWhenUnset() throws IOException {
        assertSame(DefaultPrimaryOperationPolicy.INSTANCE, engine.config().getPrimaryOperationPolicy());
        // the standard primary path still generates its own sequence numbers from zero
        Engine.IndexResult result = engine.index(indexForDoc(createParsedDoc("1", null)));
        assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
        assertEquals(0L, result.getSeqNo());
    }

    public void testExplicitDefaultProviderMatchesUnset() throws IOException {
        try (Store store = createStore()) {
            EngineConfig explicitDefault = config(defaultSettings, store, createTempDir(), newMergePolicy(), null).toBuilder()
                .primaryOperationPolicy(DefaultPrimaryOperationPolicy.INSTANCE)
                .build();
            try (InternalEngine defaultEngine = createEngine(explicitDefault)) {
                Engine.IndexResult result = defaultEngine.index(indexForDoc(createParsedDoc("1", null)));
                assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                assertEquals("an explicit default provider must generate seq nos. exactly as the implicit one does", 0L, result.getSeqNo());

                // leaves a gap at seq no. 1, which the default provider fills on promotion
                defaultEngine.index(replicaIndexForDoc(createParsedDoc("2", null), 1L, 2L, false));
                assertEquals(1, defaultEngine.fillSeqNoGaps(primaryTerm.get()));
            }
        }
    }

    public void testProviderThatAcceptsPreAssignedSeqNosPreservesThem() throws IOException {
        try (Store store = createStore()) {
            try (InternalEngine engineUnderTest = createEngine(preAssignedSeqNoConfig(store, createTempDir()))) {
                assertNotSame(DefaultPrimaryOperationPolicy.INSTANCE, engineUnderTest.config().getPrimaryOperationPolicy());

                // seq no. 7 comes from the upstream authority and must be used verbatim rather than
                // replaced by a locally generated one
                Engine.IndexResult result = engineUnderTest.index(
                    upstreamIndexForDoc(createParsedDoc("1", null), 7L, 3L, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L)
                );
                assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                assertEquals(7L, result.getSeqNo());
                assertEquals("the upstream version must be applied verbatim", 3L, result.getVersion());
                assertVisibleCount(engineUnderTest, 1);
            }
        }
    }

    /**
     * A provider may plan primary-origin operations with replica semantics, so a compare-and-swap the
     * upstream authority already resolved is not re-evaluated locally. The same operation is rejected
     * as a version conflict under the default provider.
     */
    public void testProviderCanSkipCompareAndSwapChecks() throws IOException {
        try (Store store = createStore()) {
            try (InternalEngine engineUnderTest = createEngine(preAssignedSeqNoConfig(store, createTempDir()))) {
                // ifSeqNo=99 against an empty engine is an unsatisfiable CAS precondition
                Engine.IndexResult result = engineUnderTest.index(upstreamIndexForDoc(createParsedDoc("1", null), 0L, 1L, 99L, 1L));
                assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                assertEquals(0L, result.getSeqNo());
            }
        }

        // same precondition on a default-provider engine, with the seq no. left for the engine to assign
        Engine.IndexResult conflicted = engine.index(
            upstreamIndexForDoc(createParsedDoc("1", null), SequenceNumbers.UNASSIGNED_SEQ_NO, 1L, 99L, 1L)
        );
        assertEquals(Engine.Result.Type.FAILURE, conflicted.getResultType());
        assertTrue(
            "expected a version conflict but got " + conflicted.getFailure(),
            conflicted.getFailure() instanceof VersionConflictEngineException
        );
    }

    public void testProviderCanSuppressSeqNoGapFilling() throws IOException {
        try (Store store = createStore()) {
            try (InternalEngine engineUnderTest = createEngine(preAssignedSeqNoConfig(store, createTempDir()))) {
                // leaves a gap at seq no. 0
                engineUnderTest.index(upstreamIndexForDoc(createParsedDoc("1", null), 1L, 1L, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L));
                assertEquals(
                    "a provider whose seq no. space is owned upstream must not record promotion no-ops",
                    0,
                    engineUnderTest.fillSeqNoGaps(primaryTerm.get())
                );
            }
        }
    }

    public void testDefaultProviderFillsSeqNoGaps() throws IOException {
        // leaves a gap at seq no. 0
        engine.index(replicaIndexForDoc(createParsedDoc("1", null), 1L, 1L, false));
        assertEquals(1, engine.fillSeqNoGaps(primaryTerm.get()));
    }

    /**
     * Even when the provider suppresses gap filling, {@link InternalEngine#fillSeqNoGaps(long)} must
     * still enforce {@code ensureOpen()}: a closed engine throws rather than silently returning 0.
     */
    public void testFillSeqNoGapsOnClosedEngineThrowsUnderNonDefaultProvider() throws IOException {
        try (Store store = createStore()) {
            InternalEngine engineUnderTest = createEngine(preAssignedSeqNoConfig(store, createTempDir()));
            engineUnderTest.close();
            expectThrows(AlreadyClosedException.class, () -> engineUnderTest.fillSeqNoGaps(primaryTerm.get()));
        }
    }
}
