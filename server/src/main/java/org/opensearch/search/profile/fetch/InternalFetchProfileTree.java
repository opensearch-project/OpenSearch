/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.AbstractInternalProfileTree;

/**
 * Profiling tree for fetch operations.
 */
public class InternalFetchProfileTree extends AbstractInternalProfileTree<FetchProfileBreakdown, String> {

    @Override
    protected FetchProfileBreakdown createProfileBreakdown() {
        return new FetchProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(String element) {
        return element;
    }

    @Override
    protected String getDescriptionFromElement(String element) {
        return element;
    }
}
