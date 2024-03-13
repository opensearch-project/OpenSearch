/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class SearchRequestOperationsListenerAssertingListener extends SearchRequestOperationsListener {
    private volatile SearchPhase phase;

    @Override
    protected void onPhaseStart(SearchPhaseContext context) {
        assertThat(phase, is(nullValue()));
        phase = context.getCurrentPhase();
    }

    @Override
    protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        assertThat(phase, is(context.getCurrentPhase()));
        phase = null;
    }

    @Override
    protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
        assertThat(phase, is(context.getCurrentPhase()));
        phase = null;
    }

    public void assertFinished() {
        assertThat(phase, is(nullValue()));
    }
}
