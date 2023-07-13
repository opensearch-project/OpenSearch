/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.ingest.SimulatePipelineRequest;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class IngestRequestConvertersTests extends OpenSearchTestCase {

    public void testPutPipeline() throws IOException {
        String pipelineId = "some_pipeline_id";
        PutPipelineRequest request = new PutPipelineRequest(
            "some_pipeline_id",
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomClusterManagerTimeout(request, expectedParams);
        RequestConvertersTests.setRandomTimeout(request::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request expectedRequest = IngestRequestConverters.putPipeline(request);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add("_ingest/pipeline");
        endpoint.add(pipelineId);
        Assert.assertEquals(endpoint.toString(), expectedRequest.getEndpoint());
        Assert.assertEquals(HttpPut.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testGetPipeline() {
        String pipelineId = "some_pipeline_id";
        Map<String, String> expectedParams = new HashMap<>();
        GetPipelineRequest request = new GetPipelineRequest("some_pipeline_id");
        RequestConvertersTests.setRandomClusterManagerTimeout(request, expectedParams);
        Request expectedRequest = IngestRequestConverters.getPipeline(request);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add("_ingest/pipeline");
        endpoint.add(pipelineId);
        Assert.assertEquals(endpoint.toString(), expectedRequest.getEndpoint());
        Assert.assertEquals(HttpGet.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testDeletePipeline() {
        String pipelineId = "some_pipeline_id";
        Map<String, String> expectedParams = new HashMap<>();
        DeletePipelineRequest request = new DeletePipelineRequest(pipelineId);
        RequestConvertersTests.setRandomClusterManagerTimeout(request, expectedParams);
        RequestConvertersTests.setRandomTimeout(request::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        Request expectedRequest = IngestRequestConverters.deletePipeline(request);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add("_ingest/pipeline");
        endpoint.add(pipelineId);
        Assert.assertEquals(endpoint.toString(), expectedRequest.getEndpoint());
        Assert.assertEquals(HttpDelete.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testSimulatePipeline() throws IOException {
        String pipelineId = OpenSearchTestCase.randomBoolean() ? "some_pipeline_id" : null;
        boolean verbose = OpenSearchTestCase.randomBoolean();
        String json = "{"
            + "  \"pipeline\": {"
            + "    \"description\": \"_description\","
            + "    \"processors\": ["
            + "      {"
            + "        \"set\": {"
            + "          \"field\": \"field2\","
            + "          \"value\": \"_value\""
            + "        }"
            + "      }"
            + "    ]"
            + "  },"
            + "  \"docs\": ["
            + "    {"
            + "      \"_index\": \"index\","
            + "      \"_id\": \"id\","
            + "      \"_source\": {"
            + "        \"foo\": \"rab\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        SimulatePipelineRequest request = new SimulatePipelineRequest(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        request.setId(pipelineId);
        request.setVerbose(verbose);
        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("verbose", Boolean.toString(verbose));

        Request expectedRequest = IngestRequestConverters.simulatePipeline(request);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add("_ingest/pipeline");
        if (pipelineId != null && !pipelineId.isEmpty()) endpoint.add(pipelineId);
        endpoint.add("_simulate");
        Assert.assertEquals(endpoint.toString(), expectedRequest.getEndpoint());
        Assert.assertEquals(HttpPost.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
        RequestConvertersTests.assertToXContentBody(request, expectedRequest.getEntity());
    }
}
