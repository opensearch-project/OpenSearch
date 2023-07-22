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

import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A mapper for field correlation.
 * <p>
 * A field correlation has no concrete field mappings of its own, in a similar way to the FieldAliasMapper it only points to another field within another mapping.
 * The next example shows the definition of this type of definition
 * <pre>
 * {
 *   "mappings": {
 *     "properties": {
 *       "ip_addr": {
 *         "type": "ip"
 *       },
 *       "http_ip_addr_fk": {
 *          "type": "correlation",
 *          "path": "ip_addr",
 *          "schema_pattern" : "logs*",
 *          "remote_path" : "ip"
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * @opensearch.internal
 */
public final class FieldCorrelationMapper extends FieldAliasMapper {
    public static final String CONTENT_TYPE = "correlation";

    /**
     * Parameter names
     *
     * @opensearch.internal
     */
    public static class Names {
        public static final String TARGET_SCHEMA = "schema_pattern";
        public static final String TARGET_FIELD = "remote_path";
    }

    private final String targetSchema;
    private final String targetField;

    public FieldCorrelationMapper(String simpleName, String name, String path, String targetSchema, String targetField) {
        super(simpleName, name, path);
        this.targetSchema = targetSchema;
        this.targetField = targetField;
    }

    public String contentType() {
        return CONTENT_TYPE;
    }

    public String targetSchema() {
        return targetSchema;
    }

    public String targetField() {
        return targetField;
    }

    @Override
    public Mapper merge(Mapper mergeWith) {
        if (!(mergeWith instanceof FieldCorrelationMapper)) {
            throw new IllegalArgumentException(
                "Cannot merge a field correlation mapping [" + name() + "] with a mapping that is not for a field correlation."
            );
        }
        return mergeWith;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(simpleName())
            .field("type", CONTENT_TYPE)
            .field(FieldAliasMapper.Names.PATH, super.path())
            .field(Names.TARGET_SCHEMA, targetSchema)
            .field(Names.TARGET_FIELD, targetField)
            .endObject();
    }

    @Override
    public void validate(MappingLookup mappers) {
        super.validate(mappers);
        // todo add remote schema mapping validation
    }

    /**
     * The type parser
     *
     * @opensearch.internal
     */
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            FieldCorrelationMapper.Builder builder = new FieldCorrelationMapper.Builder(name);

            Object pathField = node.remove(FieldAliasMapper.Names.PATH);
            String path = XContentMapValues.nodeStringValue(pathField, null);
            if (path == null) {
                throw new MapperParsingException("The [path] property must be specified for field [" + name + "].");
            }
            builder.path(path);

            Object targetSchemaField = node.remove(Names.TARGET_SCHEMA);
            String schema = XContentMapValues.nodeStringValue(targetSchemaField, null);
            if (schema == null) {
                throw new MapperParsingException("The [targetSchema] property must be specified for field [" + name + "].");
            }
            builder.targetSchema(schema);

            Object targetField = node.remove(Names.TARGET_FIELD);
            String target = XContentMapValues.nodeStringValue(targetField, null);
            if (target == null) {
                throw new MapperParsingException("The [targetField] property must be specified for field [" + name + "].");
            }
            return builder.targetField(target);
        }
    }

    /**
     * The builder for the field correlation field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends FieldAliasMapper.Builder {
        private String targetSchema;
        private String targetField;

        protected Builder(String name) {
            super(name);
        }

        public Builder targetSchema(String targetSchema) {
            this.targetSchema = targetSchema;
            return this;
        }

        public Builder targetField(String targetField) {
            this.targetField = targetField;
            return this;
        }

        public FieldCorrelationMapper build(BuilderContext context) {
            String fullName = context.path().pathAsText(name());
            return new FieldCorrelationMapper(name(), fullName, path, targetSchema, targetField);
        }
    }
}
