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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.client.NodeSelector;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execution context passed across the REST tests.
 * Holds the REST client used to communicate with opensearch.
 * Caches the last obtained test response and allows to stash part of it within variables
 * that can be used as input values in following requests.
 */
public class ClientYamlTestExecutionContext {

    private static final Logger logger = LogManager.getLogger(ClientYamlTestExecutionContext.class);

    private static final XContentType[] STREAMING_CONTENT_TYPES = new XContentType[] { XContentType.JSON, XContentType.SMILE };

    private final Stash stash = new Stash();
    private final ClientYamlTestClient clientYamlTestClient;

    private ClientYamlTestResponse response;

    private final boolean randomizeContentType;

    ClientYamlTestExecutionContext(ClientYamlTestClient clientYamlTestClient, boolean randomizeContentType) {
        this.clientYamlTestClient = clientYamlTestClient;
        this.randomizeContentType = randomizeContentType;
    }

    /**
     * Calls an opensearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        Map<String, String> headers
    ) throws IOException {
        return callApi(apiName, params, bodies, headers, NodeSelector.ANY);
    }

    /**
     * Calls an opensearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        Map<String, String> headers,
        NodeSelector nodeSelector
    ) throws IOException {
        // makes a copy of the parameters before modifying them for this specific request
        Map<String, String> requestParams = new HashMap<>(params);
        requestParams.putIfAbsent("error_trace", "true"); // By default ask for error traces, this my be overridden by params
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        // make a copy of the headers before modifying them for this specific request
        Map<String, String> requestHeaders = new HashMap<>(headers);
        for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        HttpEntity entity = createEntity(bodies, requestHeaders);
        try {
            response = callApiInternal(apiName, requestParams, entity, requestHeaders, nodeSelector);
            return response;
        } catch (ClientYamlTestResponseException e) {
            response = e.getRestTestResponse();
            throw e;
        } finally {
            // if we hit a bad exception the response is null
            Object responseBody = response != null ? response.getBody() : null;
            // we always stash the last response body
            stash.stashValue("body", responseBody);
        }
    }

    private HttpEntity createEntity(List<Map<String, Object>> bodies, Map<String, String> headers) throws IOException {
        if (bodies.isEmpty()) {
            return null;
        }
        if (bodies.size() == 1) {
            MediaType mediaType = getContentType(headers, XContentType.values());
            BytesRef bytesRef = bodyAsBytesRef(bodies.get(0), mediaType);
            return new ByteArrayEntity(
                bytesRef.bytes,
                bytesRef.offset,
                bytesRef.length,
                ContentType.create(mediaType.mediaTypeWithoutParameters(), StandardCharsets.UTF_8)
            );
        } else {
            MediaType mediaType = getContentType(headers, STREAMING_CONTENT_TYPES);
            List<BytesRef> bytesRefList = new ArrayList<>(bodies.size());
            int totalBytesLength = 0;
            for (Map<String, Object> body : bodies) {
                BytesRef bytesRef = bodyAsBytesRef(body, mediaType);
                bytesRefList.add(bytesRef);
                totalBytesLength += bytesRef.length - bytesRef.offset + 1;
            }
            byte[] bytes = new byte[totalBytesLength];
            int position = 0;
            for (BytesRef bytesRef : bytesRefList) {
                for (int i = bytesRef.offset; i < bytesRef.length; i++) {
                    bytes[position++] = bytesRef.bytes[i];
                }
                bytes[position++] = mediaType.xContent().streamSeparator();
            }
            return new ByteArrayEntity(bytes, ContentType.create(mediaType.mediaTypeWithoutParameters(), StandardCharsets.UTF_8));
        }
    }

    private MediaType getContentType(Map<String, String> headers, XContentType[] supportedContentTypes) {
        MediaType mediaType = null;
        String contentType = headers.get("Content-Type");
        if (contentType != null) {
            mediaType = MediaType.fromMediaType(contentType);
        }
        if (mediaType != null) {
            return mediaType;
        }
        if (randomizeContentType) {
            return RandomizedTest.randomFrom(supportedContentTypes);
        }
        return XContentType.JSON;
    }

    private BytesRef bodyAsBytesRef(Map<String, Object> bodyAsMap, MediaType mediaType) throws IOException {
        Map<String, Object> finalBodyAsMap = stash.replaceStashedValues(bodyAsMap);
        try (XContentBuilder builder = MediaTypeRegistry.contentBuilder(mediaType)) {
            return BytesReference.bytes(builder.map(finalBodyAsMap)).toBytesRef();
        }
    }

    // pkg-private for testing
    ClientYamlTestResponse callApiInternal(
        String apiName,
        Map<String, String> params,
        HttpEntity entity,
        Map<String, String> headers,
        NodeSelector nodeSelector
    ) throws IOException {
        return clientYamlTestClient.callApi(apiName, params, entity, headers, nodeSelector);
    }

    /**
     * Extracts a specific value from the last saved response
     */
    public Object response(String path) throws IOException {
        return response.evaluate(path, stash);
    }

    /**
     * Clears the last obtained response and the stashed fields
     */
    public void clear() {
        logger.debug("resetting client, response and stash");
        response = null;
        stash.clear();
    }

    public Stash stash() {
        return stash;
    }

    /**
     * Returns the current es version as a string
     */
    public Version esVersion() {
        return clientYamlTestClient.getEsVersion();
    }

    public Version clusterManagerVersion() {
        return clientYamlTestClient.getClusterManagerVersion();
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerVersion()} */
    @Deprecated
    public Version masterVersion() {
        return clusterManagerVersion();
    }

}
