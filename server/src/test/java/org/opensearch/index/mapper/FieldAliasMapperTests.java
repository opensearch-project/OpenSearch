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

public class FieldAliasMapperTests extends MapperServiceTestCase {

    public void testParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("alias-field")
            .field("type", "alias")
            .field("path", "concrete-field")
            .endObject()
            .startObject("concrete-field")
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
            () -> createDocumentMapper(mapping(b -> b.startObject("alias-field").field("type", "alias").endObject()))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: The [path] property must be specified for field [alias-field].",
            exception.getMessage()
        );
    }

    public void testParsingWithExtraArgument() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
                b.field("extra-field", "extra-value");
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping [_doc]: "
                + "Mapping definition for [alias-field] has unsupported parameters:  [extra-field : extra-value]",
            exception.getMessage()
        );
    }

    public void testMerge() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("first-field").field("type", "keyword").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "first-field");
            }
            b.endObject();
        }));

        MappedFieldType firstFieldType = mapperService.fieldType("alias-field");
        assertEquals("first-field", firstFieldType.name());
        assertTrue(firstFieldType instanceof KeywordFieldMapper.KeywordFieldType);

        merge(mapperService, mapping(b -> {
            b.startObject("second-field").field("type", "text").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "second-field");
            }
            b.endObject();
        }));

        MappedFieldType secondFieldType = mapperService.fieldType("alias-field");
        assertEquals("second-field", secondFieldType.name());
        assertTrue(secondFieldType instanceof TextFieldMapper.TextFieldType);
    }

    public void testMergeFailure() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("concrete-field").field("type", "text").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
        }));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "keyword");
            }
            b.endObject();
        })));
        assertEquals(
            "Cannot merge a field alias mapping [alias-field] with a mapping that is not for a field alias.",
            exception.getMessage()
        );
    }
}
