/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Plans execution strategies for deletion operations.
 * The planner produces {@link DeletionStrategy} instances that guide the engine's
 * execution of delete operations on both primary and replica shards.
 *
 * @opensearch.internal
 */
public class DeletionStrategyPlanner implements OperationStrategyPlanner<Engine.Delete, DeletionStrategy> {

    private final IndexSettings indexSettings;
    private final ShardId shardId;
    private final Predicate<Engine.Operation> hasBeenProcessedBefore;
    private final CheckedFunction<Engine.Operation, OpVsEngineDocStatus, IOException> opVsEngineDocStatusFunction;
    private final CheckedBiFunction<Engine.Operation, Boolean, VersionValue, IOException> docVersionSupplier;
    private final BiFunction<Engine.Operation, Integer, Exception> tryAcquireInFlightDocs;
    private final Supplier<Boolean> incrementVersionLookup;

    public DeletionStrategyPlanner(
        IndexSettings indexSettings,
        ShardId shardId,
        Predicate<Engine.Operation> hasBeenProcessedBefore,
        CheckedFunction<Engine.Operation, OpVsEngineDocStatus, IOException> opVsEngineDocStatusFunction,
        CheckedBiFunction<Engine.Operation, Boolean, VersionValue, IOException> docVersionSupplier,
        BiFunction<Engine.Operation, Integer, Exception> tryAcquireInFlightDocs,
        Supplier<Boolean> incrementVersionLookup
    ) {
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.hasBeenProcessedBefore = hasBeenProcessedBefore;
        this.opVsEngineDocStatusFunction = opVsEngineDocStatusFunction;
        this.docVersionSupplier = docVersionSupplier;
        this.tryAcquireInFlightDocs = tryAcquireInFlightDocs;
        this.incrementVersionLookup = incrementVersionLookup;
    }

    @Override
    public DeletionStrategy planOperationAsPrimary(Engine.Delete delete) throws IOException {
        assert delete.origin() == Engine.Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        // resolve operation from external to internal
        final VersionValue versionValue = docVersionSupplier.apply(delete, delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);
        assert incrementVersionLookup.get();
        final long currentVersion;
        final boolean currentlyDeleted;
        if (versionValue == null) {
            currentVersion = Versions.NOT_FOUND;
            currentlyDeleted = true;
        } else {
            currentVersion = versionValue.version;
            currentlyDeleted = versionValue.isDelete();
        }
        final DeletionStrategy plan;
        if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && currentlyDeleted) {
            final VersionConflictEngineException e = new VersionConflictEngineException(
                shardId,
                delete.id(),
                delete.getIfSeqNo(),
                delete.getIfPrimaryTerm(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            );
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, true);
        } else if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
            && (versionValue.seqNo != delete.getIfSeqNo() || versionValue.term != delete.getIfPrimaryTerm())) {
                final VersionConflictEngineException e = new VersionConflictEngineException(
                    shardId,
                    delete.id(),
                    delete.getIfSeqNo(),
                    delete.getIfPrimaryTerm(),
                    versionValue.seqNo,
                    versionValue.term
                );
                plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
            } else if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
                final VersionConflictEngineException e = new VersionConflictEngineException(
                    shardId,
                    delete,
                    currentVersion,
                    currentlyDeleted
                );
                plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
            } else {
                final Exception reserveError = tryAcquireInFlightDocs.apply(delete, 1);
                if (reserveError != null) {
                    plan = DeletionStrategy.failAsTooManyDocs(reserveError);
                } else {
                    final long versionOfDeletion = delete.versionType().updateVersion(currentVersion, delete.version());
                    plan = DeletionStrategy.processNormally(currentlyDeleted, versionOfDeletion, 1);
                }
            }
        return plan;
    }

    @Override
    public DeletionStrategy planOperationAsNonPrimary(Engine.Delete delete) throws IOException {
        assert assertNonPrimaryOrigin(delete);
        final DeletionStrategy plan;
        if (hasBeenProcessedBefore.test(delete)) {
            // the operation seq# was processed thus this operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = DeletionStrategy.processButSkipEngine(false, delete.version());
        } else {
            boolean segRepEnabled = indexSettings.isSegRepEnabledOrRemoteNode();
            final OpVsEngineDocStatus opVsLucene = opVsEngineDocStatusFunction.apply(delete);
            if (opVsLucene == OpVsEngineDocStatus.OP_STALE_OR_EQUAL) {
                if (segRepEnabled) {
                    // For segrep based indices, we can't completely rely on localCheckpointTracker
                    // as the preserved checkpoint may not have all the operations present in lucene
                    // we don't need to index it again as stale op as it would create multiple documents for same seq no
                    plan = DeletionStrategy.processButSkipEngine(false, delete.version());
                } else {
                    plan = DeletionStrategy.processAsStaleOp(delete.version());
                }
            } else {
                plan = DeletionStrategy.processNormally(opVsLucene == OpVsEngineDocStatus.DOC_NOT_FOUND, delete.version(), 0);
            }
        }
        return plan;
    }
}
