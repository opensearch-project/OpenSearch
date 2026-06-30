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

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentTooLongException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.impl.BasicEntityDetails;
import org.apache.hc.core5.http.io.entity.AbstractHttpEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.opensearch.client.nio.HeapBufferedAsyncResponseConsumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class HeapBufferedAsyncResponseConsumerTests extends RestClientTestCase {

    // maximum buffer that this test ends up allocating is 50MB
    private static final int MAX_TEST_BUFFER_SIZE = 50 * 1024 * 1024;
    private static final int TEST_BUFFER_LIMIT = 10 * 1024 * 1024;

    public void testDefaultBufferLimit() throws Exception {
        HeapBufferedAsyncResponseConsumer consumer = new HeapBufferedAsyncResponseConsumer(TEST_BUFFER_LIMIT);
        bufferLimitTest(consumer, TEST_BUFFER_LIMIT);
    }

    public void testConfiguredBufferLimit() throws Exception {
        try {
            new HeapBufferedAsyncResponseConsumer(randomIntBetween(Integer.MIN_VALUE, 0));
        } catch (IllegalArgumentException e) {
            assertEquals("bufferLimit must be greater than 0", e.getMessage());
        }
        try {
            new HeapBufferedAsyncResponseConsumer(0);
        } catch (IllegalArgumentException e) {
            assertEquals("bufferLimit must be greater than 0", e.getMessage());
        }
        int bufferLimit = randomIntBetween(1, MAX_TEST_BUFFER_SIZE - 100);
        HeapBufferedAsyncResponseConsumer consumer = new HeapBufferedAsyncResponseConsumer(bufferLimit);
        bufferLimitTest(consumer, bufferLimit);
    }

    public void testCanConfigureHeapBufferLimitFromOutsidePackage() throws ClassNotFoundException, NoSuchMethodException,
        IllegalAccessException, InvocationTargetException, InstantiationException {
        int bufferLimit = randomIntBetween(1, Integer.MAX_VALUE);
        // we use reflection to make sure that the class can be instantiated from the outside, and the constructor is public
        Constructor<?> constructor = HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.class.getConstructor(
            Integer.TYPE
        );
        assertEquals(Modifier.PUBLIC, constructor.getModifiers() & Modifier.PUBLIC);
        Object object = constructor.newInstance(bufferLimit);
        assertThat(object, instanceOf(HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.class));
        HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
            (HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory) object;
        AsyncResponseConsumer<ClassicHttpResponse> consumer = consumerFactory.createHttpAsyncResponseConsumer();
        assertThat(consumer, instanceOf(HeapBufferedAsyncResponseConsumer.class));
        HeapBufferedAsyncResponseConsumer bufferedAsyncResponseConsumer = (HeapBufferedAsyncResponseConsumer) consumer;
        assertEquals(bufferLimit, bufferedAsyncResponseConsumer.getBufferLimit());
    }

    public void testHttpAsyncResponseConsumerFactoryVisibility() throws ClassNotFoundException {
        assertEquals(Modifier.PUBLIC, HttpAsyncResponseConsumerFactory.class.getModifiers() & Modifier.PUBLIC);
    }

    private static void bufferLimitTest(HeapBufferedAsyncResponseConsumer consumer, int bufferLimit) throws Exception {
        HttpContext httpContext = mock(HttpContext.class);

        BasicClassicHttpResponse response = new BasicClassicHttpResponse(200, "OK");
        consumer.consumeResponse(response, null, httpContext, null);

        final AtomicReference<Long> contentLength = new AtomicReference<>();
        HttpEntity entity = new AbstractHttpEntity(ContentType.APPLICATION_JSON, null, false) {
            @Override
            public long getContentLength() {
                return contentLength.get();
            }

            @Override
            public InputStream getContent() throws IOException, UnsupportedOperationException {
                return new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public boolean isStreaming() {
                return false;
            }

            @Override
            public void close() throws IOException {}
        };
        contentLength.set(randomLongBetween(0L, bufferLimit));
        response.setEntity(entity);

        final EntityDetails details = new BasicEntityDetails(4096, ContentType.APPLICATION_JSON);
        consumer.consumeResponse(response, details, httpContext, null);

        contentLength.set(randomLongBetween(bufferLimit + 1, MAX_TEST_BUFFER_SIZE));
        try {
            consumer.consumeResponse(response, details, httpContext, null);
        } catch (ContentTooLongException e) {
            assertEquals(
                "entity content is too long [" + entity.getContentLength() + "] for the configured buffer limit [" + bufferLimit + "]",
                e.getMessage()
            );
        }
    }
}
