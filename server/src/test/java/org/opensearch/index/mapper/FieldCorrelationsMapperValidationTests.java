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

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Explicit;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class FieldCorrelationsMapperValidationTests extends OpenSearchTestCase {

    public void testDuplicateFieldCorrelationAndObject() {
        ObjectMapper objectMapper = createObjectMapper("some.path");
        FieldCorrelationMapper correlationMapper = new FieldCorrelationMapper("path", "some.path", "field", "remoteSchema", "fieldFK");

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> new MappingLookup(emptyList(), singletonList(objectMapper), singletonList(correlationMapper), 0, Lucene.STANDARD_ANALYZER)
        );
        assertEquals("correlation [some.path] is defined both as an object and an correlation", e.getMessage());
    }

    public void testDuplicateFieldCorrelationAndConcreteField() {
        FieldMapper field = new MockFieldMapper("field");
        FieldMapper invalidField = new MockFieldMapper("invalid");
        FieldCorrelationMapper invalidCorrelation = new FieldCorrelationMapper("invalid", "invalid", "field", "remoteSchema", "fieldFK");

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> new MappingLookup(
                Arrays.asList(field, invalidField),
                emptyList(),
                singletonList(invalidCorrelation),
                0,
                Lucene.STANDARD_ANALYZER
            )
        );

        assertEquals("correlation [invalid] is defined both as an correlation and a concrete field", e.getMessage());
    }

    public void testCorrelationThatRefersToCorrelation() {
        FieldMapper field = new MockFieldMapper("field");
        FieldCorrelationMapper correlation = new FieldCorrelationMapper("correlation", "correlation", "field", "remoteSchema", "fieldFK");
        FieldCorrelationMapper invalidCorrelation = new FieldCorrelationMapper(
            "invalid-correlation",
            "invalid-correlation",
            "correlation",
            "remoteSchema",
            "fieldFK"
        );

        MappingLookup mappers = new MappingLookup(
            singletonList(field),
            emptyList(),
            Arrays.asList(correlation, invalidCorrelation),
            0,
            Lucene.STANDARD_ANALYZER
        );
        correlation.validate(mappers);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> invalidCorrelation.validate(mappers));

        assertEquals(
            "Invalid [path] value [correlation] for field correlation [invalid-correlation]: an correlation"
                + " cannot refer to another correlation.",
            e.getMessage()
        );
    }

    public void testCorrelationThatRefersToItself() {
        FieldCorrelationMapper invalidCorrelation = new FieldCorrelationMapper(
            "invalid-correlation",
            "invalid-correlation",
            "invalid-correlation",
            "remoteSchema",
            "fieldFK"
        );

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = new MappingLookup(emptyList(), emptyList(), singletonList(invalidCorrelation), 0, null);
            invalidCorrelation.validate(mappers);
        });

        assertEquals(
            "Invalid [path] value [invalid-correlation] for field correlation [invalid-correlation]: an correlation"
                + " cannot refer to itself.",
            e.getMessage()
        );
    }

    public void testCorrelationWithNonExistentPath() {
        FieldCorrelationMapper invalidCorrelation = new FieldCorrelationMapper(
            "invalid-correlation",
            "invalid-correlation",
            "non-existent",
            "remoteSchema",
            "fieldFK"
        );

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                emptyList(),
                emptyList(),
                singletonList(invalidCorrelation),
                0,
                Lucene.STANDARD_ANALYZER
            );
            invalidCorrelation.validate(mappers);
        });

        assertEquals(
            "Invalid [path] value [non-existent] for field correlation [invalid-correlation]: an correlation"
                + " must refer to an existing field in the mappings.",
            e.getMessage()
        );
    }

    public void testFieldCorrelationWithNestedScope() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldCorrelationMapper correlationMapper = new FieldCorrelationMapper(
            "correlation",
            "nested.correlation",
            "nested.field",
            "remoteSchema",
            "fieldFK"
        );

        MappingLookup mappers = new MappingLookup(
            singletonList(createFieldMapper("nested", "field")),
            singletonList(objectMapper),
            singletonList(correlationMapper),
            0,
            Lucene.STANDARD_ANALYZER
        );
        correlationMapper.validate(mappers);
    }

    public void testFieldCorrelationWithDifferentObjectScopes() {

        FieldCorrelationMapper correlationMapper = new FieldCorrelationMapper(
            "correlation",
            "object2.correlation",
            "object1.field",
            "remoteSchema",
            "fieldFK"
        );

        MappingLookup mappers = new MappingLookup(
            singletonList(createFieldMapper("object1", "field")),
            Arrays.asList(createObjectMapper("object1"), createObjectMapper("object2")),
            singletonList(correlationMapper),
            0,
            Lucene.STANDARD_ANALYZER
        );
        correlationMapper.validate(mappers);
    }

    public void testFieldCorrelationWithNestedTarget() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldCorrelationMapper correlationMapper = new FieldCorrelationMapper(
            "correlation",
            "correlation",
            "nested.field",
            "remoteSchema",
            "fieldFK"
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                singletonList(createFieldMapper("nested", "field")),
                singletonList(objectMapper),
                singletonList(correlationMapper),
                0,
                Lucene.STANDARD_ANALYZER
            );
            correlationMapper.validate(mappers);
        });

        String expectedMessage = "Invalid [path] value [nested.field] for field correlation [correlation]: "
            + "an correlation must have the same nested scope as its target. The correlation is not nested, "
            + "but the target's nested scope is [nested].";
        assertEquals(expectedMessage, e.getMessage());
    }

    public void testFieldCorrelationWithDifferentNestedScopes() {
        FieldCorrelationMapper correlationMapper = new FieldCorrelationMapper(
            "correlation",
            "nested2.correlation",
            "nested1.field",
            "remoteSchema",
            "fieldFK"
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                singletonList(createFieldMapper("nested1", "field")),
                Arrays.asList(createNestedObjectMapper("nested1"), createNestedObjectMapper("nested2")),
                singletonList(correlationMapper),
                0,
                Lucene.STANDARD_ANALYZER
            );
            correlationMapper.validate(mappers);
        });

        String expectedMessage = "Invalid [path] value [nested1.field] for field correlation [nested2.correlation]: "
            + "an correlation must have the same nested scope as its target. The correlation's nested scope is [nested2], "
            + "but the target's nested scope is [nested1].";
        assertEquals(expectedMessage, e.getMessage());
    }

    private static final Settings SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
        .build();

    private static FieldMapper createFieldMapper(String parent, String name) {
        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(parent));
        return new BooleanFieldMapper.Builder(name).build(context);
    }

    private static ObjectMapper createObjectMapper(String name) {
        return new ObjectMapper(
            name,
            name,
            new Explicit<>(true, false),
            ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.FALSE,
            emptyMap(),
            SETTINGS
        );
    }

    private static ObjectMapper createNestedObjectMapper(String name) {
        return new ObjectMapper(
            name,
            name,
            new Explicit<>(true, false),
            ObjectMapper.Nested.newNested(),
            ObjectMapper.Dynamic.FALSE,
            emptyMap(),
            SETTINGS
        );
    }
}
