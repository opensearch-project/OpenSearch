/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.OpenSearchParseException;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.stats.SortBy;
import org.opensearch.wlm.stats.SortOrder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestWlmStatsActionTests extends OpenSearchTestCase {

    private RestWlmStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestWlmStatsAction();
    }

    public void testParsePageSizeDefault() {
        RestRequest request = mock(RestRequest.class);
        when(request.paramAsInt("size", 10)).thenReturn(10);
        assertEquals(10, action.parsePageSize(request));
    }

    public void testParsePageSizeValid() {
        RestRequest request = mock(RestRequest.class);
        when(request.paramAsInt("size", 10)).thenReturn(25);
        assertEquals(25, action.parsePageSize(request));
    }

    public void testParsePageSizeNegative() {
        RestRequest request = mock(RestRequest.class);
        when(request.paramAsInt("size", 10)).thenReturn(-5);
        expectThrows(OpenSearchParseException.class, () -> action.parsePageSize(request));
    }

    public void testParsePageSizeTooLarge() {
        RestRequest request = mock(RestRequest.class);
        when(request.paramAsInt("size", 10)).thenReturn(102);
        expectThrows(OpenSearchParseException.class, () -> action.parsePageSize(request));
    }

    public void testParseSortByValid() {
        assertEquals(SortBy.NODE_ID, action.parseSortBy("node_id"));
        assertEquals(SortBy.WORKLOAD_GROUP, action.parseSortBy("workload_group"));
    }

    public void testParseSortByInvalid() {
        expectThrows(OpenSearchParseException.class, () -> action.parseSortBy("invalid_key"));
    }

    public void testParseSortOrderValid() {
        assertEquals(SortOrder.ASC, action.parseSortOrder("asc"));
        assertEquals(SortOrder.DESC, action.parseSortOrder("desc"));
    }

    public void testParseSortOrderInvalid() {
        expectThrows(OpenSearchParseException.class, () -> action.parseSortOrder("upside_down"));
    }
}
