/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.junit.Before;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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

    public void testCreatePit() throws IOException {
        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "index");
        CreatePitResponse pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse.getId() != null);
        assertEquals(1, pitResponse.getTotalShards());
        assertEquals(1, pitResponse.getSuccessfulShards());
        assertEquals(0, pitResponse.getFailedShards());
        assertEquals(0, pitResponse.getSkippedShards());
    }
    /**
     * Todo: add deletion logic and test cluster settings
     */
}
