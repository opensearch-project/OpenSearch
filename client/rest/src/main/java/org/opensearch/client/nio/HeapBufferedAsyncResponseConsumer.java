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

package org.opensearch.client.nio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.support.AbstractAsyncResponseConsumer;
import org.apache.hc.core5.http.protocol.HttpContext;

import java.io.IOException;

/**
 * Default implementation of {@link AsyncResponseConsumer}. Buffers the whole
 * response content in heap memory, meaning that the size of the buffer is equal to the content-length of the response.
 * Limits the size of responses that can be read based on a configurable argument. Throws an exception in case the entity is longer
 * than the configured buffer limit.
 */
public class HeapBufferedAsyncResponseConsumer extends AbstractAsyncResponseConsumer<ClassicHttpResponse, byte[]> {
    private static final Log LOGGER = LogFactory.getLog(HeapBufferedAsyncResponseConsumer.class);
    private final int bufferLimit;

    /**
     * Creates a new instance of this consumer with the provided buffer limit.
     *
     * @param bufferLimit the buffer limit. Must be greater than 0.
     * @throws IllegalArgumentException if {@code bufferLimit} is less than or equal to 0.
     */
    public HeapBufferedAsyncResponseConsumer(int bufferLimit) {
        super(new HeapBufferedAsyncEntityConsumer(bufferLimit));
        this.bufferLimit = bufferLimit;
    }

    /**
     * Get the limit of the buffer.
     */
    public int getBufferLimit() {
        return bufferLimit;
    }

    /**
     * Triggered to signal receipt of an intermediate (1xx) HTTP response.
     *
     * @param response the intermediate (1xx) HTTP response.
     * @param context the actual execution context.
     */
    @Override
    public void informationResponse(final HttpResponse response, final HttpContext context) throws HttpException, IOException {}

    /**
     * Triggered to generate object that represents a result of response message processing.
     * @param response the response message.
     * @param entity the response entity.
     * @param contentType the response content type.
     * @return the result of response processing.
     */
    @Override
    protected ClassicHttpResponse buildResult(final HttpResponse response, final byte[] entity, final ContentType contentType) {
        final ClassicHttpResponse classicResponse = new BasicClassicHttpResponse(response.getCode());
        classicResponse.setVersion(response.getVersion());
        classicResponse.setHeaders(response.getHeaders());
        classicResponse.setReasonPhrase(response.getReasonPhrase());
        if (response.getLocale() != null) {
            classicResponse.setLocale(response.getLocale());
        }

        if (entity != null) {
            String encoding = null;

            try {
                final Header contentEncoding = response.getHeader(HttpHeaders.CONTENT_ENCODING);
                if (contentEncoding != null) {
                    encoding = contentEncoding.getValue();
                }
            } catch (final HttpException ex) {
                LOGGER.debug("Unable to detect content encoding", ex);
            }

            final ByteArrayEntity httpEntity = new ByteArrayEntity(entity, contentType, encoding);
            classicResponse.setEntity(httpEntity);
        }

        return classicResponse;
    }
}
