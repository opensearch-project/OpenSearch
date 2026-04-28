/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.profile.ProfileResult;

import java.util.List;

/**
 * Profiler for the fetch phase using a simplified profiling tree.
 */
@ExperimentalApi()
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
     * Get the {@link FetchProfileBreakdown} for a fetch sub-phase under the specified parent.
     */
    public FetchProfileBreakdown startSubPhase(String element, String parentElement) {
        return profileTree.startSubPhase(element, parentElement);
    }

    /**
     * Finish profiling of the specified fetch phase.
     */
    public void endFetchPhase(String element) {
        profileTree.endFetchPhase(element);
    }

    /**
     * @return a hierarchical representation of the profiled tree.
     */
    public List<ProfileResult> getTree() {
        return profileTree.getTree();
    }
}
