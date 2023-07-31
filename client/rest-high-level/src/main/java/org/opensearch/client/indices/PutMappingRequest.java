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

package org.opensearch.client.indices;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.TimedRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Put a mapping definition into one or more indices. If an index already contains mappings,
 * the new mappings will be merged with the existing one. If there are elements that cannot
 * be merged, the request will be rejected.
 *
 * @opensearch.api
 */
public class PutMappingRequest extends TimedRequest implements IndicesRequest, ToXContentObject {

    private final String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);

    private BytesReference source;
    private MediaType mediaType;

    /**
     * Constructs a new put mapping request against one or more indices. If no indices
     * are provided then it will be executed against all indices.
     */
    public PutMappingRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * The indices into which the mappings will be put.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public PutMappingRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * The mapping source definition.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * The {@link MediaType} of the mapping source.
     */
    public MediaType mediaType() {
        return mediaType;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(Map<String, ?> mappingSource) {
        try {
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.getDefaultMediaType());
            builder.map(mappingSource);
            return source(builder);
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + mappingSource + "]", e);
        }
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(String mappingSource, MediaType mediaType) {
        this.source = new BytesArray(mappingSource);
        this.mediaType = mediaType;
        return this;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(XContentBuilder builder) {
        this.source = BytesReference.bytes(builder);
        this.mediaType = builder.contentType();
        return this;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(BytesReference source, MediaType mediaType) {
        this.source = source;
        this.mediaType = mediaType;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (source != null) {
            try (InputStream stream = source.streamInput()) {
                builder.rawValue(stream, mediaType);
            }
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
