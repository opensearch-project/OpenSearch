/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

/** The single-alias endpoint carries the scoring mode into the alias action. */
public class RestIndexPutAliasActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexPutAliasAction());
    }

    public void testCarriesTheScoringModeMode() {
        AtomicReference<IndicesAliasesRequest> sent = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((action, request) -> {
            sent.set((IndicesAliasesRequest) request);
            return null;
        });
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.PUT)
            .withPath("/idx/_alias/scoped")
            .withContent(new BytesArray("{\"filter\":{\"term\":{\"f\":\"v\"}},\"scoring_mode\":\"no_stats\"}"), MediaTypeRegistry.JSON)
            .build();
        dispatchRequest(request);
        assertNotNull("the handler sends an alias request", sent.get());
        assertEquals("no_stats", sent.get().getAliasActions().get(0).scoringMode());
    }
}
