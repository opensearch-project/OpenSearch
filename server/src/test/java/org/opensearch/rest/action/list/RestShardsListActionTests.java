/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.pagination.PaginationStrategy;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;
import static org.opensearch.rest.action.list.RestShardsListAction.MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE;
import static org.opensearch.rest.action.list.RestShardsListAction.MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE;

public class RestShardsListActionTests extends OpenSearchTestCase {

    private final RestShardsListAction action = new RestShardsListAction();

    public void testShardsListActionIsPaginated() {
        assertTrue(action.isActionPaginated());
    }

    public void testValidateAndGetPageParamsWithDefaultParams() {
        Map<String, String> params = new HashMap<>();
        RestRequest restRequest = new FakeRestRequest(params);
        PageParams pageParams = action.validateAndGetPageParams(restRequest);
        assertEquals(MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE, pageParams.getSize());
        assertEquals(PARAM_ASC_SORT_VALUE, pageParams.getSort());
        assertNull(pageParams.getRequestedToken());
    }

    public void testValidateAndGetPageParamsWithSizeBelowMin() {
        Map<String, String> params = new HashMap<>();
        params.put("size", String.valueOf(MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE - 1));
        RestRequest restRequest = new FakeRestRequest(params);
        assertThrows(IllegalArgumentException.class, () -> action.validateAndGetPageParams(restRequest));
    }

    public void testValidateAndGetPageParamsWithSizeAboveRange() {
        Map<String, String> params = new HashMap<>();
        params.put("size", String.valueOf(MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE * 10));
        RestRequest restRequest = new FakeRestRequest(params);
        assertThrows(IllegalArgumentException.class, () -> action.validateAndGetPageParams(restRequest));
    }

    public void testValidateAndGetPageParamsWithInvalidRequestToken() {
        Map<String, String> params = new HashMap<>();
        params.put("next_token", PaginationStrategy.encryptStringToken("1|-1|test"));
        RestRequest restRequest = new FakeRestRequest(params);
        assertThrows(OpenSearchParseException.class, () -> action.validateAndGetPageParams(restRequest));
    }

    public void testValidateAndGetPageParamsWithValidPageParams() {
        Map<String, String> params = new HashMap<>();
        params.put("next_token", PaginationStrategy.encryptStringToken("1|1|test"));
        params.put("sort", "asc");
        params.put("size", "3000");
        RestRequest restRequest = new FakeRestRequest(params);
        PageParams pageParams = action.validateAndGetPageParams(restRequest);

        assertEquals(PaginationStrategy.encryptStringToken("1|1|test"), pageParams.getRequestedToken());
        assertEquals(3000, pageParams.getSize());
        assertEquals("asc", pageParams.getSort());
    }

}
