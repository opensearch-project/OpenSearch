/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class IndexAwareSearchRequestScopeTests extends OpenSearchTestCase {

    public void testSearchWithoutRemoteIndicesHasNoRemoteIndices() {
        assertFalse(IndexAwareSearchRequestScope.hasRemoteIndices(Map.of()));
    }

    // The check takes no ccs_minimize_roundtrips argument on purpose: a remote index excludes the search
    // in either mode, so there is a single path to test here.
    public void testSearchWithARemoteIndexHasRemoteIndices() {
        assertTrue(IndexAwareSearchRequestScope.hasRemoteIndices(remoteIndices("remote-index")));
    }

    private static Map<String, OriginalIndices> remoteIndices(String... indices) {
        return Map.of("my-remote-cluster", new OriginalIndices(indices, IndicesOptions.LENIENT_EXPAND_OPEN));
    }
}
