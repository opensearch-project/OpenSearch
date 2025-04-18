/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {
    // public void testPersistUpdatedRule_Success() throws IOException {
    // IndexStoredRulePersistenceService indexStoredRulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // Client client = indexStoredRulePersistenceService.getClient();
    // UpdateResponse updateResponse = mock(UpdateResponse.class);
    // when(updateResponse.status()).thenReturn(RestStatus.OK);
    // ActionListener<UpdateRuleResponse> listener = mock(ActionListener.class);
    // ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
    // doAnswer(invocation -> {
    // ActionListener<UpdateResponse> callback = invocation.getArgument(1);
    // callback.onResponse(updateResponse);
    // return null;
    // }).when(client).update(requestCaptor.capture(), any());
    // indexStoredRulePersistenceService.persistUpdatedRule(_ID_ONE, ruleTwo, listener);
    // verify(listener, times(1)).onResponse(any(UpdateRuleResponse.class));
    // verify(listener, never()).onFailure(any());
    // assertEquals(_ID_ONE, requestCaptor.getValue().id());
    // }
    //
    // public void testPersistUpdatedRule_UpdateFailure() {
    // IndexStoredRulePersistenceService indexStoredRulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // Client client = indexStoredRulePersistenceService.getClient();
    // ActionListener<UpdateRuleResponse> listener = mock(ActionListener.class);
    // doAnswer(invocation -> {
    // ActionListener<UpdateResponse> callback = invocation.getArgument(1);
    // callback.onFailure(new Exception("Update failure"));
    // return null;
    // }).when(client).update(any(UpdateRequest.class), any());
    // indexStoredRulePersistenceService.persistUpdatedRule(_ID_ONE, ruleTwo, listener);
    //
    // verify(listener, times(1)).onFailure(any(Exception.class));
    // verify(listener, never()).onResponse(any());
    // }
}
