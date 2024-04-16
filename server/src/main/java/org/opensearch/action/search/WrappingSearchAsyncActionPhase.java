/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.search.SearchPhaseResult;

/**
 * The WrappingSearchAsyncActionPhase (see please {@link CanMatchPreFilterSearchPhase} as one example) is a special case
 * of search phase that wraps SearchAsyncActionPhase as {@link SearchPhase}. The {@link AbstractSearchAsyncAction} manages own
 * onPhaseStart / onPhaseFailure / OnPhaseDone callbacks and but just wrapping it with the SearchPhase causes
 * only some callbacks being called. The {@link AbstractSearchAsyncAction} has special  treatment of {@link WrappingSearchAsyncActionPhase}.
 */
class WrappingSearchAsyncActionPhase extends SearchPhase {
    private final AbstractSearchAsyncAction<? extends SearchPhaseResult> action;

    protected WrappingSearchAsyncActionPhase(AbstractSearchAsyncAction<? extends SearchPhaseResult> action) {
        super(action.getName());
        this.action = action;
    }

    @Override
    public void run() {
        action.start();
    }

    SearchPhase getSearchPhase() {
        return action;
    }
}
