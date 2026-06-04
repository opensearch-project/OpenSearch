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
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.opensearch.client.nio.HeapBufferedAsyncResponseConsumer;

import static org.opensearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.DEFAULT_BUFFER_LIMIT;

/**
 * Factory used to create instances of {@link AsyncResponseConsumer}. Each request retry needs its own instance of the
 * consumer object. Users can implement this interface and pass their own instance to the specialized
 * performRequest methods that accept an {@link HttpAsyncResponseConsumerFactory} instance as argument.
 */
public interface HttpAsyncResponseConsumerFactory {

    /**
     * Creates the default type of {@link AsyncResponseConsumer}, based on heap buffering with a buffer limit of 100MB.
     */
    HttpAsyncResponseConsumerFactory DEFAULT = new HeapBufferedResponseConsumerFactory(DEFAULT_BUFFER_LIMIT);

    /**
     * Creates the {@link AsyncResponseConsumer}, called once per request attempt.
     */
    AsyncResponseConsumer<ClassicHttpResponse> createHttpAsyncResponseConsumer();

    /**
     * Default factory used to create instances of {@link AsyncResponseConsumer}.
     * Creates one instance of {@link HeapBufferedAsyncResponseConsumer} for each request attempt, with a configurable
     * buffer limit which defaults to 100MB.
     */
    class HeapBufferedResponseConsumerFactory implements HttpAsyncResponseConsumerFactory {

        // default buffer limit is 100MB
        static final int DEFAULT_BUFFER_LIMIT = 100 * 1024 * 1024;

        private final int bufferLimit;

        /**
         * Creates a {@link HeapBufferedResponseConsumerFactory} instance with the given buffer limit.
         *
         * @param bufferLimitBytes the buffer limit to be applied to this instance
         */
        public HeapBufferedResponseConsumerFactory(int bufferLimitBytes) {
            this.bufferLimit = bufferLimitBytes;
        }

        /**
         * Creates the {@link AsyncResponseConsumer}, called once per request attempt.
         */
        @Override
        public AsyncResponseConsumer<ClassicHttpResponse> createHttpAsyncResponseConsumer() {
            return new HeapBufferedAsyncResponseConsumer(bufferLimit);
        }
    }
}
