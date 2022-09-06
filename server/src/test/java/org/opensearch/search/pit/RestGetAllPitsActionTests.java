/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for rest get all PITs action
 */
public class RestGetAllPitsActionTests extends OpenSearchTestCase {

    // public void testGetAllPits() throws Exception {
    // SetOnce<Boolean> pitCalled = new SetOnce<>();
    // try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
    //// @Override
    //// public void getAllPits(GetAllPitNodesRequest request, ActionListener<GetAllPitNodesResponse> listener) {
    //// pitCalled.set(true);
    //// }
    // }) {
    // RestGetAllPitsAction action = new RestGetAllPitsAction(new Supplier<DiscoveryNodes>() {
    // @Override
    // public DiscoveryNodes get() {
    // return null;
    // }
    // });
    // RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_all").build();
    // FakeRestChannel channel = new FakeRestChannel(request, false, 0);
    // action.handleRequest(request, channel, nodeClient);
    // assertThat(pitCalled.get(), equalTo(true));
    // }
    // }
}
