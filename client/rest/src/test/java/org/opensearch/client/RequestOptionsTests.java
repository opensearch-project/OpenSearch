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

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class RequestOptionsTests extends RestClientTestCase {
    public void testDefault() {
        assertEquals(Collections.<Header>emptyList(), RequestOptions.DEFAULT.getHeaders());
        assertEquals(HttpAsyncResponseConsumerFactory.DEFAULT, RequestOptions.DEFAULT.getHttpAsyncResponseConsumerFactory());
        assertEquals(RequestOptions.DEFAULT, RequestOptions.DEFAULT.toBuilder().build());
    }

    public void testAddHeader() {
        try {
            randomBuilder().addHeader(null, randomAsciiLettersOfLengthBetween(3, 10));
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("header name cannot be null", e.getMessage());
        }

        try {
            randomBuilder().addHeader(randomAsciiLettersOfLengthBetween(3, 10), null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("header value cannot be null", e.getMessage());
        }

        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        int numHeaders = between(0, 5);
        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < numHeaders; i++) {
            Header header = new RequestOptions.ReqHeader(randomAsciiAlphanumOfLengthBetween(5, 10), randomAsciiAlphanumOfLength(3));
            headers.add(header);
            builder.addHeader(header.getName(), header.getValue());
        }
        RequestOptions options = builder.build();
        assertEquals(headers, options.getHeaders());

        try {
            options.getHeaders()
                .add(new RequestOptions.ReqHeader(randomAsciiAlphanumOfLengthBetween(5, 10), randomAsciiAlphanumOfLength(3)));
            fail("expected failure");
        } catch (UnsupportedOperationException e) {
            assertNull(e.getMessage());
        }
    }

    public void testAddParameter() {
        assertThrows(
            "query parameter name cannot be null",
            NullPointerException.class,
            () -> randomBuilder().addParameter(null, randomAsciiLettersOfLengthBetween(3, 10))
        );

        assertThrows(
            "query parameter value cannot be null",
            NullPointerException.class,
            () -> randomBuilder().addParameter(randomAsciiLettersOfLengthBetween(3, 10), null)
        );

        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        int numParameters = between(0, 5);
        Map<String, String> parameters = new HashMap<>();
        for (int i = 0; i < numParameters; i++) {
            String name = randomAsciiAlphanumOfLengthBetween(5, 10);
            String value = randomAsciiAlphanumOfLength(3);
            parameters.put(name, value);
            builder.addParameter(name, value);
        }
        RequestOptions options = builder.build();
        assertEquals(parameters, options.getParameters());

        try {
            options.getParameters().put(randomAsciiAlphanumOfLengthBetween(5, 10), randomAsciiAlphanumOfLength(3));
            fail("expected failure");
        } catch (UnsupportedOperationException e) {
            assertNull(e.getMessage());
        }
    }

    public void testSetHttpAsyncResponseConsumerFactory() {
        try {
            RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("httpAsyncResponseConsumerFactory cannot be null", e.getMessage());
        }

        HttpAsyncResponseConsumerFactory factory = mock(HttpAsyncResponseConsumerFactory.class);
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(factory);
        RequestOptions options = builder.build();
        assertSame(factory, options.getHttpAsyncResponseConsumerFactory());
    }

    public void testSetRequestBuilder() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        Timeout responseTimeout = Timeout.ofMilliseconds(10000);
        Timeout connectTimeout = Timeout.ofMilliseconds(100);
        requestConfigBuilder.setResponseTimeout(responseTimeout).setConnectTimeout(connectTimeout);
        RequestConfig requestConfig = requestConfigBuilder.build();

        builder.setRequestConfig(requestConfig);
        RequestOptions options = builder.build();
        assertSame(options.getRequestConfig(), requestConfig);
        assertEquals(options.getRequestConfig().getResponseTimeout(), responseTimeout);
        assertEquals(options.getRequestConfig().getConnectTimeout(), connectTimeout);
    }

    public void testEqualsAndHashCode() {
        RequestOptions request = randomBuilder().build();
        assertEquals(request, request);

        RequestOptions copy = copy(request);
        assertEquals(request, copy);
        assertEquals(copy, request);
        assertEquals(request.hashCode(), copy.hashCode());

        RequestOptions mutant = mutate(request);
        assertNotEquals(request, mutant);
        assertNotEquals(mutant, request);
    }

    static RequestOptions.Builder randomBuilder() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        if (randomBoolean()) {
            int headerCount = between(1, 5);
            for (int i = 0; i < headerCount; i++) {
                builder.addHeader(randomAsciiAlphanumOfLength(3), randomAsciiAlphanumOfLength(3));
            }
        }

        if (randomBoolean()) {
            int queryParamCount = between(1, 5);
            for (int i = 0; i < queryParamCount; i++) {
                builder.addParameter(randomAsciiAlphanumOfLength(3), randomAsciiAlphanumOfLength(3));
            }
        }

        if (randomBoolean()) {
            builder.setHttpAsyncResponseConsumerFactory(new HeapBufferedResponseConsumerFactory(1));
        }

        if (randomBoolean()) {
            builder.setWarningsHandler(randomBoolean() ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        }

        if (randomBoolean()) {
            builder.setRequestConfig(RequestConfig.custom().build());
        }

        return builder;
    }

    private static RequestOptions copy(RequestOptions options) {
        return options.toBuilder().build();
    }

    private static RequestOptions mutate(RequestOptions options) {
        RequestOptions.Builder mutant = options.toBuilder();
        int mutationType = between(0, 2);
        switch (mutationType) {
            case 0:
                mutant.addHeader("extra", "m");
                return mutant.build();
            case 1:
                mutant.setHttpAsyncResponseConsumerFactory(new HeapBufferedResponseConsumerFactory(5));
                return mutant.build();
            case 2:
                mutant.setWarningsHandler(new WarningsHandler() {
                    @Override
                    public boolean warningsShouldFailRequest(List<String> warnings) {
                        fail("never called");
                        return false;
                    }
                });
                return mutant.build();
            default:
                throw new UnsupportedOperationException("Unknown mutation type [" + mutationType + "]");
        }
    }
}
