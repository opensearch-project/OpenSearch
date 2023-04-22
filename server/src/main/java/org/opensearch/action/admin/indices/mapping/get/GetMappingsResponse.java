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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport response to get field mappings.
 *
 * @opensearch.internal
 */
public class GetMappingsResponse extends ActionResponse implements ToXContentFragment {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final Map<String, MappingMetadata> mappings;

    public GetMappingsResponse(final Map<String, MappingMetadata> mappings) {
        this.mappings = Collections.unmodifiableMap(mappings);
    }

    GetMappingsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        final Map<String, MappingMetadata> indexMapBuilder = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                int mappingCount = in.readVInt();
                if (mappingCount == 0) {
                    indexMapBuilder.put(index, MappingMetadata.EMPTY_MAPPINGS);
                } else if (mappingCount == 1) {
                    String type = in.readString();
                    if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
                        throw new IllegalStateException("Expected " + MapperService.SINGLE_MAPPING_NAME + " but got [" + type + "]");
                    }
                    indexMapBuilder.put(index, new MappingMetadata(in));
                } else {
                    throw new IllegalStateException("Expected 0 or 1 mappings but got: " + mappingCount);
                }
            } else {
                boolean hasMapping = in.readBoolean();
                indexMapBuilder.put(index, hasMapping ? new MappingMetadata(in) : MappingMetadata.EMPTY_MAPPINGS);
            }
        }
        mappings = Collections.unmodifiableMap(indexMapBuilder);
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(mappings.size());
        for (Map.Entry<String, MappingMetadata> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeVInt(indexEntry.getValue() == MappingMetadata.EMPTY_MAPPINGS ? 0 : 1);
                if (indexEntry.getValue() != MappingMetadata.EMPTY_MAPPINGS) {
                    out.writeString(MapperService.SINGLE_MAPPING_NAME);
                    indexEntry.getValue().writeTo(out);
                }
            } else {
                out.writeOptionalWriteable(indexEntry.getValue());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (final Map.Entry<String, MappingMetadata> indexEntry : getMappings().entrySet()) {
            builder.startObject(indexEntry.getKey());
            if (indexEntry.getValue() != null) {
                builder.field(MAPPINGS.getPreferredName(), indexEntry.getValue().sourceAsMap());
            } else {
                builder.startObject(MAPPINGS.getPreferredName()).endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public int hashCode() {
        return mappings.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        GetMappingsResponse other = (GetMappingsResponse) obj;
        return this.mappings.equals(other.mappings);
    }
}
