/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests point in time API with rest high level client
 */
public class PitIT extends OpenSearchRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/_doc/1");
        doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
        client().performRequest(doc1);
        Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/_doc/2");
        doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
        client().performRequest(doc2);
        Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/_doc/3");
        doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
        client().performRequest(doc3);
        Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/_doc/4");
        doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
        client().performRequest(doc4);
        Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/_doc/5");
        doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
        client().performRequest(doc5);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testCreateAndDeletePit() throws IOException {
        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "index");
        CreatePitResponse createPitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(createPitResponse.getId() != null);
        assertEquals(1, createPitResponse.getTotalShards());
        assertEquals(1, createPitResponse.getSuccessfulShards());
        assertEquals(0, createPitResponse.getFailedShards());
        assertEquals(0, createPitResponse.getSkippedShards());
        GetAllPitNodesResponse getAllPitResponse = highLevelClient().getAllPits(RequestOptions.DEFAULT);
        List<String> pits = getAllPitResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
        assertTrue(pits.contains(createPitResponse.getId()));
        List<String> pitIds = new ArrayList<>();
        pitIds.add(createPitResponse.getId());
        DeletePitRequest deletePitRequest = new DeletePitRequest(pitIds);
        DeletePitResponse deletePitResponse = execute(deletePitRequest, highLevelClient()::deletePit, highLevelClient()::deletePitAsync);
        assertTrue(deletePitResponse.getDeletePitResults().get(0).isSuccessful());
        assertTrue(deletePitResponse.getDeletePitResults().get(0).getPitId().equals(createPitResponse.getId()));
    }

    public void testDeleteAllAndListAllPits() throws Exception {
        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "index");
        CreatePitResponse pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        CreatePitResponse pitResponse1 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse.getId() != null);
        assertTrue(pitResponse1.getId() != null);
        DeletePitResponse deletePitResponse = highLevelClient().deleteAllPits(RequestOptions.DEFAULT);
        for (DeletePitInfo deletePitInfo : deletePitResponse.getDeletePitResults()) {
            assertTrue(deletePitInfo.isSuccessful());
        }
        pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        pitResponse1 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse.getId() != null);
        assertTrue(pitResponse1.getId() != null);
        GetAllPitNodesResponse getAllPitResponse = highLevelClient().getAllPits(RequestOptions.DEFAULT);

        List<String> pits = getAllPitResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
        assertTrue(pits.contains(pitResponse.getId()));
        assertTrue(pits.contains(pitResponse1.getId()));
        ActionListener<DeletePitResponse> deletePitListener = new ActionListener<>() {
            @Override
            public void onResponse(DeletePitResponse response) {
                for (DeletePitInfo deletePitInfo : response.getDeletePitResults()) {
                    assertTrue(deletePitInfo.isSuccessful());
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (!(e instanceof OpenSearchStatusException)) {
                    throw new AssertionError("Delete all failed");
                }
            }
        };
        final CreatePitResponse pitResponse3 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse3.getId() != null);
        ActionListener<GetAllPitNodesResponse> getPitsListener = new ActionListener<GetAllPitNodesResponse>() {
            @Override
            public void onResponse(GetAllPitNodesResponse response) {
                List<String> pits = response.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
                assertTrue(pits.contains(pitResponse3.getId()));
                // delete all pits
                highLevelClient().deleteAllPitsAsync(RequestOptions.DEFAULT, deletePitListener);
            }

            @Override
            public void onFailure(Exception e) {
                if (!(e instanceof OpenSearchStatusException)) {
                    throw new AssertionError("List all PITs failed", e);
                }
            }
        };
        highLevelClient().getAllPitsAsync(RequestOptions.DEFAULT, getPitsListener);

        // validate no pits case
        assertBusy(() -> {
            GetAllPitNodesResponse getAllPitResponse1 = highLevelClient().getAllPits(RequestOptions.DEFAULT);
            assertTrue(getAllPitResponse1.getPitInfos().size() == 0);
            highLevelClient().deleteAllPitsAsync(RequestOptions.DEFAULT, deletePitListener);
        });
    }
}
