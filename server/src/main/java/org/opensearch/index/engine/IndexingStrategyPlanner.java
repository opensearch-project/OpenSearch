/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.index.AppendOnlyIndexOperationRetryException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.exec.bridge.Indexer;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class IndexingStrategyPlanner implements OperationStrategyPlanner {

    private final EngineConfig engineConfig;
    private final ShardId shardId;
    private final LiveVersionMap versionMap;
    private final Supplier<Long> maxUnsafeAutoIdTimestampSupplier;
    private final Supplier<Long> maxSeqNoOfUpdatesOrDeletesSupplier;
    private final Supplier<Long> processedCheckpointSupplier;
    private final Predicate<Engine.Operation> hasBeenProcessedBefore;
    private final CheckedFunction<Engine.Operation, Indexer.OpVsEngineDocStatus, IOException> opVsEngineDocStatusFunction;
    private final CheckedBiFunction<Engine.Operation, Boolean, VersionValue, IOException> docVersionSupplier;
    private final BiConsumer<Long, Boolean> updateAutoIdTimestampConsumer;
    private final BiFunction<Engine.Operation, Integer, Exception> tryAcquireInFlightDocs;

    public IndexingStrategyPlanner(
        EngineConfig engineConfig,
        ShardId shardId,
        LiveVersionMap versionMap,
        Supplier<Long> maxUnsafeAutoIdTimestampSupplier,
        Supplier<Long> maxSeqNoOfUpdatesOrDeletesSupplier,
        Supplier<Long> processedCheckpointSupplier,
        Predicate<Engine.Operation> hasBeenProcessedBefore,
        CheckedFunction<Engine.Operation, Indexer.OpVsEngineDocStatus, IOException> opVsEngineDocStatusFunction,
        CheckedBiFunction<Engine.Operation, Boolean, VersionValue, IOException> docVersionSupplier,
        BiConsumer<Long, Boolean> updateAutoIdTimestampConsumer,
        BiFunction<Engine.Operation, Integer, Exception> tryAcquireInFlightDocs
    ) {
        this.engineConfig = engineConfig;
        this.shardId = shardId;
        this.versionMap = versionMap;
        this.maxUnsafeAutoIdTimestampSupplier = maxUnsafeAutoIdTimestampSupplier;
        this.maxSeqNoOfUpdatesOrDeletesSupplier = maxSeqNoOfUpdatesOrDeletesSupplier;
        this.processedCheckpointSupplier = processedCheckpointSupplier;
        this.hasBeenProcessedBefore = hasBeenProcessedBefore;
        this.opVsEngineDocStatusFunction = opVsEngineDocStatusFunction;
        this.docVersionSupplier = docVersionSupplier;
        this.updateAutoIdTimestampConsumer = updateAutoIdTimestampConsumer;
        this.tryAcquireInFlightDocs = tryAcquireInFlightDocs;
    }

    @Override
    public IndexingStrategy planOperationAsPrimary(Engine.Operation operation) throws IOException {
        final Engine.Index index = (Engine.Index) operation;
        assert index.origin() == Engine.Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final int reservingDocs = index.parsedDoc().docs().size();
        final IndexingStrategy plan;
        // resolve an external operation into an internal one which is safe to replay
        final boolean canOptimizeAddDocument = canOptimizeAddDocument(index);
        if (canOptimizeAddDocument && mayHaveBeenIndexedBefore(index) == false) {
            final Exception reserveError = tryAcquireInFlightDocs.apply(index, reservingDocs);
            if (reserveError != null) {
                plan = IndexingStrategy.failAsTooManyDocs(reserveError);
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(1L, reservingDocs);
            }
        } else {
            versionMap.enforceSafeAccess();
            // resolves incoming version
            final VersionValue versionValue = docVersionSupplier.apply(index, true);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && currentNotFoundOrDeleted) {
                final VersionConflictEngineException e = new VersionConflictEngineException(
                    shardId,
                    index.id(),
                    index.getIfSeqNo(),
                    index.getIfPrimaryTerm(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                );
                plan = IndexingStrategy.skipDueToVersionConflict(e, true, currentVersion);
            } else if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (versionValue.seqNo != index.getIfSeqNo()
                || versionValue.term != index.getIfPrimaryTerm())) {
                final VersionConflictEngineException e = new VersionConflictEngineException(
                    shardId,
                    index.id(),
                    index.getIfSeqNo(),
                    index.getIfPrimaryTerm(),
                    versionValue.seqNo,
                    versionValue.term
                );
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else if (index.versionType().isVersionConflictForWrites(currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e =
                    new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else {
                final Exception reserveError = tryAcquireInFlightDocs.apply(index, reservingDocs);
                if (reserveError != null) {
                    plan = IndexingStrategy.failAsTooManyDocs(reserveError);
                } else if (currentVersion >= 1 && engineConfig.getIndexSettings().getIndexMetadata().isAppendOnlyIndex()) {
                    // Retry happens for indexing requests for append only indices, since we are rejecting update requests
                    // at Transport layer itself. So for any retry, we are reconstructing response from already indexed
                    // document version for append only index.
                    AppendOnlyIndexOperationRetryException retryException =
                        new AppendOnlyIndexOperationRetryException("Indexing operation retried for append only indices");
                    final Engine.IndexResult result =
                        new Engine.IndexResult(retryException, currentVersion, versionValue.term, versionValue.seqNo);
                    plan = IndexingStrategy.failAsIndexAppendOnly(result, currentVersion, 0);
                } else {
                    plan = IndexingStrategy.processNormally(
                        currentNotFoundOrDeleted,
                        canOptimizeAddDocument ? 1L : index.versionType().updateVersion(currentVersion, index.version()),
                        reservingDocs
                    );
                }
            }
        }
        return plan;
    }

    @Override
    public IndexingStrategy planOperationAsNonPrimary(Engine.Operation operation) throws IOException {
        final Engine.Index index = (Engine.Index) operation;
        assert index.origin() != Engine.Operation.Origin.PRIMARY : "planing as primary but got " + index.origin();
        // needs to maintain the auto_id timestamp in case this replica becomes primary
        if (canOptimizeAddDocument(index)) {
            mayHaveBeenIndexedBefore(index);
        }
        final IndexingStrategy plan;
        // unlike the primary, replicas don't really care to about creation status of documents
        // this allows to ignore the case where a document was found in the live version maps in
        // a delete state and return false for the created flag in favor of code simplicity
        final long maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletesSupplier.get();
        if (hasBeenProcessedBefore.test(index)) {
            // the operation seq# was processed and thus the same operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = IndexingStrategy.processButSkipEngine(false, index.version());
        } else if (maxSeqNoOfUpdatesOrDeletes <= processedCheckpointSupplier.get()) {
            // see Engine#getMaxSeqNoOfUpdatesOrDeletes for the explanation of the optimization using sequence numbers
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : index.seqNo() + ">=" + maxSeqNoOfUpdatesOrDeletes;
            plan = IndexingStrategy.optimizedAppendOnly(index.version(), 0);
        } else {
            boolean segRepEnabled = engineConfig.getIndexSettings().isSegRepEnabledOrRemoteNode();
            versionMap.enforceSafeAccess();
            final Indexer.OpVsEngineDocStatus opVsLucene = opVsEngineDocStatusFunction.apply(index);
            if (opVsLucene == Indexer.OpVsEngineDocStatus.OP_STALE_OR_EQUAL) {
                if (segRepEnabled) {
                    // For segrep based indices, we can't completely rely on localCheckpointTracker
                    // as the preserved checkpoint may not have all the operations present in lucene
                    // we don't need to index it again as stale op as it would create multiple documents for same seq no
                    plan = IndexingStrategy.processButSkipEngine(false, index.version());
                } else {
                    plan = IndexingStrategy.processAsStaleOp(index.version());
                }
            } else {
                plan = IndexingStrategy.processNormally(opVsLucene == Indexer.OpVsEngineDocStatus.DOC_NOT_FOUND, index.version(), 0);
            }
        }
        return plan;
    }

    private boolean canOptimizeAddDocument(Engine.Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert
                index.getAutoGeneratedIdTimestamp() >= 0 :
                "autoGeneratedIdTimestamp must be positive but was: " + index.getAutoGeneratedIdTimestamp();
            return switch (index.origin()) {
                case PRIMARY -> {
                    assert assertPrimaryCanOptimizeAddDocument(index);
                    yield true;
                }
                case PEER_RECOVERY, REPLICA -> {
                    assert
                        index.version() == 1 && index.versionType() == null :
                        "version: " + index.version() + " type: " + index.versionType();
                    yield true;
                }
                case LOCAL_TRANSLOG_RECOVERY, LOCAL_RESET -> {
                    assert index.isRetry();
                    yield true;
                }
                default -> throw new IllegalArgumentException("unknown origin " + index.origin());
            };
        }
        return false;
    }

    protected boolean assertPrimaryCanOptimizeAddDocument(final Engine.Index index) {
        assert (index.version() == Versions.MATCH_DELETED || index.version() == Versions.MATCH_ANY)
            && index.versionType() == VersionType.INTERNAL : "version: " + index.version() + " type: " + index.versionType();
        return true;
    }

    /**
     * returns true if the indexing operation may have already be processed by this engine.
     * Note that it is OK to rarely return true even if this is not the case. However a `false`
     * return value must always be correct.
     *
     */
    private boolean mayHaveBeenIndexedBefore(Engine.Index index) {
        assert canOptimizeAddDocument(index);
        final boolean mayHaveBeenIndexBefore;
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
            updateAutoIdTimestampConsumer.accept(index.getAutoGeneratedIdTimestamp(), true);
            assert maxUnsafeAutoIdTimestampSupplier.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            // in this case we force
            mayHaveBeenIndexBefore = maxUnsafeAutoIdTimestampSupplier.get() >= index.getAutoGeneratedIdTimestamp();
            updateAutoIdTimestampConsumer.accept(index.getAutoGeneratedIdTimestamp(), false);
        }
        return mayHaveBeenIndexBefore;
    }
}
