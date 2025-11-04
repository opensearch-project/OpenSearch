/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.io.IOException;
import java.util.Collection;

public class MergeScheduler {

    private MergeHandler mergeHandler;
    private CompositeEngine compositeEngine;

    public MergeScheduler(MergeHandler mergeHandler, CompositeEngine compositeEngine) {
        this.mergeHandler = mergeHandler;
        this.compositeEngine = compositeEngine;
    }

    public void triggerMerges() throws IOException {
        // TODO: Move the merge to seperate thread
        Collection<OneMerge> oneMerges = mergeHandler.findMerges();

        // TODO: For now it is serial. we can make it concurrent if needed, similar to concurrent/serial merge scheduler

        for(OneMerge oneMerge : oneMerges) {
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            this.compositeEngine.applyMergeChanges(mergeResult, oneMerge);
        }
    }

    public void forceMerge(int maxNumSegment) throws IOException {
        // TODO: Add validation for background merge before executing force merge
        Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);

        for(OneMerge oneMerge : oneMerges) {
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            this.compositeEngine.applyMergeChanges(mergeResult, oneMerge);
        }
    }
}
