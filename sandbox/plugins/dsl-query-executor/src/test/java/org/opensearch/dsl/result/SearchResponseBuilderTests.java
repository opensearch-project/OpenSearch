/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SearchResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildReturnsEmptyResponse() {
        SearchResponse response = SearchResponseBuilder.build(List.of(), 42L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(42L, response.getTook().millis());
    }
}
