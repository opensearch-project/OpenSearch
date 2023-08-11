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

package org.opensearch.index.mapper;

import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class FieldCorrelationMapperTests extends MapperServiceTestCase {

    public void testParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("correlation-field")
            .field("type", "correlation")
            .field("path", "first-field")
            .field("schema_pattern", "schema")
            .field("remote_path", "remote_field")
            .endObject()
            .startObject("first-field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper mapper = createDocumentMapper("_doc", mapping);
        assertEquals(mapping, mapper.mappingSource().toString());
    }

    public void testParsingWithMissingPath() {
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mapping(b -> b.startObject("correlation-field").field("type", "correlation").endObject()))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: The [path] property must be specified for field [correlation-field].",
            exception.getMessage()
        );
    }

    public void testParsingWithoutSchemaFieldArgument() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "concrete-field");
                b.field("extra-field", "extra-value");
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping [_doc]: " + "The [targetSchema] property must be specified for field [correlation-field].",
            exception.getMessage()
        );
    }

    public void testParsingWithoutTargetFieldArgument() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "concrete-field");
                b.field("schema_pattern", "concrete-schema-pattern");
                b.field("extra-field", "extra-value");
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping [_doc]: " + "The [targetField] property must be specified for field [correlation-field].",
            exception.getMessage()
        );
    }

    public void testParsingWitExtraFieldArgument() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "concrete-field");
                b.field("schema_pattern", "concrete-schema-pattern");
                b.field("remote_path", "concrete-schema-pattern");
                b.field("extra-field", "extra-value");
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping [_doc]: "
                + "Mapping definition for [correlation-field] has unsupported parameters:  [extra-field : extra-value]",
            exception.getMessage()
        );
    }

    public void testMerge() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("first-field").field("type", "keyword").endObject();
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "first-field");
                b.field("schema_pattern", "index1*");
                b.field("remote_path", "remote-field1");
            }
            b.endObject();
        }));

        MappedFieldType firstFieldType = mapperService.fieldType("correlation-field");
        assertEquals("first-field", firstFieldType.name());
        assertTrue(firstFieldType instanceof KeywordFieldMapper.KeywordFieldType);

        merge(mapperService, mapping(b -> {
            b.startObject("second-field").field("type", "text").endObject();
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "second-field");
                b.field("schema_pattern", "index2*");
                b.field("remote_path", "remote-field2");
            }
            b.endObject();
        }));

        MappedFieldType secondFieldType = mapperService.fieldType("correlation-field");
        assertEquals("second-field", secondFieldType.name());
        assertTrue(secondFieldType instanceof TextFieldMapper.TextFieldType);
    }

    public void testMergeFailure() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("concrete-field").field("type", "text").endObject();
            b.startObject("correlation-field");
            {
                b.field("type", "correlation");
                b.field("path", "concrete-field");
                b.field("remote_path", "remote-field");
                b.field("schema_pattern", "index*");
            }
            b.endObject();
        }));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("correlation-field");
            {
                b.field("type", "keyword");
            }
            b.endObject();
        })));
        assertEquals(
            "Cannot merge a field correlation mapping [correlation-field] with a mapping that is not for a field correlation.",
            exception.getMessage()
        );
    }
}
