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

package org.opensearch.index.mapper;

import java.util.Objects;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;

/**
 * Stores the document source
 *
 * @opensearch.internal
 */
public class SourceToParse {

    private final BytesReference source;

    private final String index;

    private final String id;

    private final @Nullable String routing;

    private final MediaType mediaType;

    public SourceToParse(String index, String id, BytesReference source, MediaType mediaType, @Nullable String routing) {
        this.index = Objects.requireNonNull(index);
        this.id = Objects.requireNonNull(id);
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.source = new BytesArray(Objects.requireNonNull(source).toBytesRef());
        this.mediaType = Objects.requireNonNull(mediaType);
        this.routing = routing;
    }

    public SourceToParse(String index, String id, BytesReference source, MediaType mediaType) {
        this(index, id, source, mediaType, null);
    }

    public BytesReference source() {
        return this.source;
    }

    public String index() {
        return this.index;
    }

    public String id() {
        return this.id;
    }

    public @Nullable String routing() {
        return this.routing;
    }

    public MediaType getMediaType() {
        return this.mediaType;
    }

    /**
     * Origin of the source
     *
     * @opensearch.internal
     */
    public enum Origin {
        PRIMARY,
        REPLICA
    }
}
