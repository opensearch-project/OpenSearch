/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.ProfileResult;

import java.util.List;

/**
 * Profiler for the fetch phase using a simplified profiling tree.
 */
@PublicApi(since = "3.2.0")
public class FetchProfiler {
    private final FlatFetchProfileTree profileTree;

    /** Creates a new FetchProfiler. */
    public FetchProfiler() {
        this.profileTree = new FlatFetchProfileTree();
    }

    /**
     * Get the {@link FetchProfileBreakdown} for the given element in the tree,
     * potentially creating it if it did not exist.
     */
    public FetchProfileBreakdown startFetchPhase(String element) {
        return profileTree.startFetchPhase(element);
    }

    /**
     * Get the {@link FetchProfileBreakdown} for a fetch sub-phase.
     */
    public FetchProfileBreakdown startSubPhase(String element) {
        return profileTree.startSubPhase(element);
    }

    /**
     * Finish profiling of the current fetch phase.
     */
    public void endCurrentFetchPhase() {
        profileTree.endCurrentFetchPhase();
    }

    /**
     * @return a hierarchical representation of the profiled tree.
     */
    public List<ProfileResult> getTree() {
        return profileTree.getTree();
    }
}
