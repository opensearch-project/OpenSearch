/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.lucene.uid.Versions;

/**
 * The indexing strategy
 *
 * @opensearch.internal
 */
public class IndexingStrategy extends OperationStrategy {

    public final boolean currentNotFoundOrDeleted;
    public final boolean optimizeAppendOnly;

    private IndexingStrategy(
        boolean currentNotFoundOrDeleted,
        boolean optimizeAppendOnly,
        boolean indexIntoEngine,
        boolean addStaleOpToEngine,
        long versionForIndexing,
        int reservedDocs,
        Engine.IndexResult earlyResultOnPreFlightError
    ) {
        super(indexIntoEngine, addStaleOpToEngine, versionForIndexing, earlyResultOnPreFlightError, reservedDocs);
        assert (indexIntoEngine && earlyResultOnPreFlightError != null) == false :
            "can only index into engine or have a preflight result but not both." + "indexIntoEngine: " + indexIntoEngine
                + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
        assert reservedDocs == 0 || indexIntoEngine || addStaleOpToEngine : reservedDocs;
        this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
        this.optimizeAppendOnly = optimizeAppendOnly;
    }

    static IndexingStrategy optimizedAppendOnly(long versionForIndexing, int reservedDocs) {
        return new IndexingStrategy(true, false, true, false, versionForIndexing, reservedDocs, null);
    }

    public static IndexingStrategy skipDueToVersionConflict(
        VersionConflictEngineException e,
        boolean currentNotFoundOrDeleted,
        long currentVersion
    ) {
        final Engine.IndexResult result = new Engine.IndexResult(e, currentVersion);
        return new IndexingStrategy(currentNotFoundOrDeleted, false, false, false, Versions.NOT_FOUND, 0, result);
    }

    static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted, long versionForIndexing, int reservedDocs) {
        return new IndexingStrategy(
            currentNotFoundOrDeleted,
            currentNotFoundOrDeleted == false,
            true,
            false,
            versionForIndexing,
            reservedDocs,
            null
        );
    }

    public static IndexingStrategy processButSkipEngine(boolean currentNotFoundOrDeleted, long versionForIndexing) {
        return new IndexingStrategy(currentNotFoundOrDeleted, false, false, false, versionForIndexing, 0, null);
    }

    static IndexingStrategy processAsStaleOp(long versionForIndexing) {
        return new IndexingStrategy(false, false, false, true, versionForIndexing, 0, null);
    }

    static IndexingStrategy failAsTooManyDocs(Exception e) {
        final Engine.IndexResult result = new Engine.IndexResult(e, Versions.NOT_FOUND);
        return new IndexingStrategy(false, false, false, false, Versions.NOT_FOUND, 0, result);
    }

    static IndexingStrategy failAsIndexAppendOnly(Engine.IndexResult result, long versionForIndexing, int reservedDocs) {
        return new IndexingStrategy(false, false, false, true, versionForIndexing, reservedDocs, result);
    }
}
