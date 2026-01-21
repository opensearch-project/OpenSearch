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

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VerifiableWriteable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.metadata.index.model.MappingMetadataModel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MappingMetadata extends AbstractDiffable<MappingMetadata> implements VerifiableWriteable {
    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap());

    private final MappingMetadataModel model;

    public MappingMetadata(MappingMetadataModel model) {
        this.model = model;
    }

    public MappingMetadata(DocumentMapper docMapper) {
        this.model = new MappingMetadataModel(
            docMapper.type(),
            docMapper.mappingSource().compressedData(),
            docMapper.routingFieldMapper().required()
        );
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(CompressedXContent mapping) {
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        String type = mappingMap.keySet().iterator().next();
        boolean routingRequired = isRoutingRequired((Map<String, Object>) mappingMap.get(type));
        this.model = new MappingMetadataModel(type, mapping.compressedData(), routingRequired);
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(String type, Map<String, Object> mapping) {
        CompressedXContent source;
        try {
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(mapping);
            source = new CompressedXContent(BytesReference.bytes(mappingBuilder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);  // XContent exception, should never happen
        }
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        boolean routingRequired = isRoutingRequired(withoutType);

        this.model = new MappingMetadataModel(type, source.compressedData(), routingRequired);
    }

    @SuppressWarnings("unchecked")
    private boolean isRoutingRequired(Map<String, Object> withoutType) {
        boolean required = false;
        if (withoutType.containsKey("_routing")) {
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_routing");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    try {
                        required = nodeBooleanValue(fieldNode);
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException(
                            "Failed to create mapping for type [" + this.type() + "]. " + "Illegal value in field [_routing.required].",
                            ex
                        );
                    }
                }
            }
        }
        return required;
    }

    public String type() {
        return model.type();
    }

    public CompressedXContent source() {
        return new CompressedXContent(model.source());
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> sourceAsMap() throws OpenSearchParseException {
        Map<String, Object> mapping = XContentHelper.convertToMap(source().compressedReference(), true).v2();
        if (mapping.size() == 1 && mapping.containsKey(type())) {
            // the type name is the root value, reduce it
            mapping = (Map<String, Object>) mapping.get(type());
        }
        return mapping;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> getSourceAsMap() throws OpenSearchParseException {
        return sourceAsMap();
    }

    public boolean routingRequired() {
        return model.routingRequired();
    }

    public MappingMetadataModel model() {
        return model;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        model.writeTo(out);
    }

    @Override
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        out.writeString(type());
        source().writeVerifiableTo(out);
        out.writeBoolean(routingRequired());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;
        return Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
        return model.hashCode();
    }

    public MappingMetadata(StreamInput in) throws IOException {
        this.model = new MappingMetadataModel(in);
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }
}
