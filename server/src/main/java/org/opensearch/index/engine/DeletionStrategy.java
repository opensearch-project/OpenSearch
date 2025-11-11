/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.seqno.SequenceNumbers;

public class DeletionStrategy extends OperationStrategy {

    public final boolean currentlyDeleted;

    public DeletionStrategy(
        boolean deleteFromLucene,
        boolean addStaleOpToEngine,
        boolean currentlyDeleted,
        long version,
        int reservedDocs,
        Engine.DeleteResult earlyResultOnPreflightError
    ) {
        super(deleteFromLucene, addStaleOpToEngine, version, earlyResultOnPreflightError, reservedDocs);
        assert (deleteFromLucene && earlyResultOnPreflightError != null) == false :
            "can only delete from lucene or have a preflight result but not both." + "deleteFromLucene: " + deleteFromLucene
                + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
        this.currentlyDeleted = currentlyDeleted;
    }

    public static DeletionStrategy skipDueToVersionConflict(
        VersionConflictEngineException e,
        long currentVersion,
        boolean currentlyDeleted
    ) {
        final Engine.DeleteResult deleteResult = new Engine.DeleteResult(
            e,
            currentVersion,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            currentlyDeleted == false
        );
        return new DeletionStrategy(false, false, currentlyDeleted, Versions.NOT_FOUND, 0, deleteResult);
    }

    static DeletionStrategy processNormally(boolean currentlyDeleted, long versionOfDeletion, int reservedDocs) {
        return new DeletionStrategy(true, false, currentlyDeleted, versionOfDeletion, reservedDocs, null);

    }

    public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long versionOfDeletion) {
        return new DeletionStrategy(false, false, currentlyDeleted, versionOfDeletion, 0, null);
    }

    static DeletionStrategy processAsStaleOp(long versionOfDeletion) {
        return new DeletionStrategy(false, true, false, versionOfDeletion, 0, null);
    }

    static DeletionStrategy failAsTooManyDocs(Exception e) {
        final Engine.DeleteResult deleteResult = new Engine.DeleteResult(
            e,
            Versions.NOT_FOUND,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            false
        );
        return new DeletionStrategy(false, false, false, Versions.NOT_FOUND, 0, deleteResult);
    }
}
