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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response object for {@link GetFieldMappingsRequest} API
 * <p>
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should go to that client class as well.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetFieldMappingsResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private static final ObjectParser<Map<String, Map<String, FieldMappingMetadata>>, String> PARSER = new ObjectParser<>(
        MAPPINGS.getPreferredName(),
        true,
        HashMap::new
    );

    static {
        PARSER.declareField((p, typeMappings, index) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, FieldMappingMetadata> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);

                    while (p.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String fieldName = p.currentName();
                        final FieldMappingMetadata fieldMappingMetadata = FieldMappingMetadata.fromXContent(p);
                        typeMapping.put(fieldName, fieldMappingMetadata);
                    }
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private final Map<String, Map<String, FieldMappingMetadata>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, FieldMappingMetadata>> mappings) {
        this.mappings = mappings;
    }

    GetFieldMappingsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        Map<String, Map<String, FieldMappingMetadata>> indexMapBuilder = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                int typesSize = in.readVInt();
                // if the requested field doesn't exist, type size in the received response from 1.x node is 0
                if (typesSize == 0) {
                    indexMapBuilder.put(index, Collections.emptyMap());
                    continue;
                }
                if (typesSize != 1) {
                    throw new IllegalStateException("Expected single type but received [" + typesSize + "]");
                }
                in.readString(); // type
            }
            int fieldSize = in.readVInt();
            Map<String, FieldMappingMetadata> fieldMapBuilder = new HashMap<>(fieldSize);
            for (int k = 0; k < fieldSize; k++) {
                fieldMapBuilder.put(in.readString(), new FieldMappingMetadata(in.readString(), in.readBytesReference()));
            }
            indexMapBuilder.put(index, unmodifiableMap(fieldMapBuilder));
        }
        mappings = unmodifiableMap(indexMapBuilder);
    }

    /** returns the retrieved field mapping. The return map keys are index, type, field (as specified in the request). */
    public Map<String, Map<String, FieldMappingMetadata>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetadata for the requested field or null if not found.
     */
    public FieldMappingMetadata fieldMappings(String index, String field) {
        Map<String, FieldMappingMetadata> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        return indexMapping.get(field);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Map<String, FieldMappingMetadata>> indexEntry : mappings.entrySet()) {
            builder.startObject(indexEntry.getKey());
            builder.startObject(MAPPINGS.getPreferredName());

            if (mappings != null) {
                addFieldMappingsToBuilder(builder, params, indexEntry.getValue());
            }

            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void addFieldMappingsToBuilder(XContentBuilder builder, Params params, Map<String, FieldMappingMetadata> mappings)
        throws IOException {
        for (Map.Entry<String, FieldMappingMetadata> fieldEntry : mappings.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            fieldEntry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }

    /**
     * Metadata for field mappings for toXContent
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class FieldMappingMetadata implements ToXContentFragment {

        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetadata, String> PARSER = new ConstructingObjectParser<>(
            "field_mapping_meta_data",
            true,
            a -> new FieldMappingMetadata((String) a[0], (BytesReference) a[1])
        );

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(optionalConstructorArg(), (p, c) -> {
                final XContentBuilder jsonBuilder = jsonBuilder().copyCurrentStructure(p);
                final BytesReference bytes = BytesReference.bytes(jsonBuilder);
                return bytes;
            }, MAPPING, ObjectParser.ValueType.OBJECT);
        }

        private final String fullName;
        private final BytesReference source;

        public FieldMappingMetadata(String fullName, BytesReference source) {
            this.fullName = fullName;
            this.source = source;
        }

        public String fullName() {
            return fullName;
        }

        /** Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}. */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true, MediaTypeRegistry.JSON).v2();
        }

        // pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        public static FieldMappingMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(FULL_NAME.getPreferredName(), fullName);
            if (params.paramAsBoolean("pretty", false)) {
                builder.field("mapping", sourceAsMap());
            } else {
                try (InputStream stream = source.streamInput()) {
                    builder.rawField(MAPPING.getPreferredName(), stream, MediaTypeRegistry.JSON);
                }
            }
            return builder;
        }

        @Override
        public String toString() {
            return "FieldMappingMetadata{fullName='" + fullName + '\'' + ", source=" + source + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldMappingMetadata)) return false;
            FieldMappingMetadata that = (FieldMappingMetadata) o;
            return Objects.equals(fullName, that.fullName) && Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(mappings.size());
        for (Map.Entry<String, Map<String, FieldMappingMetadata>> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeVInt(1);
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeVInt(indexEntry.getValue().size());
            for (Map.Entry<String, FieldMappingMetadata> fieldEntry : indexEntry.getValue().entrySet()) {
                out.writeString(fieldEntry.getKey());
                FieldMappingMetadata fieldMapping = fieldEntry.getValue();
                out.writeString(fieldMapping.fullName());
                out.writeBytesReference(fieldMapping.source);
            }
        }
    }

    @Override
    public String toString() {
        return "GetFieldMappingsResponse{" + "mappings=" + mappings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetFieldMappingsResponse)) return false;
        GetFieldMappingsResponse that = (GetFieldMappingsResponse) o;
        return Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }

}
