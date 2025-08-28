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

package org.opensearch.search.suggest.completion;

import org.apache.lucene.index.IndexableField;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.search.suggest.completion.context.ContextBuilder;
import org.opensearch.search.suggest.completion.context.ContextMapping;
import org.opensearch.search.suggest.completion.context.GeoContextMapping;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geometry.utils.Geohash.addNeighborsAtLevel;
import static org.opensearch.search.suggest.completion.CategoryContextMappingTests.assertContextSuggestFields;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GeoContextMappingTests extends OpenSearchSingleNodeTestCase {

    public void testIndexingWithNoContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startArray("completion")
                            .startObject()
                            .array("input", "suggestion1", "suggestion2")
                            .field("weight", 3)
                            .endObject()
                            .startObject()
                            .array("input", "suggestion3", "suggestion4")
                            .field("weight", 4)
                            .endObject()
                            .startObject()
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .field("weight", 5)
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startArray("completion")
                            .startObject()
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .startObject("contexts")
                            .startObject("ctx")
                            .field("lat", 43.6624803)
                            .field("lon", -79.3863353)
                            .endObject()
                            .endObject()
                            .field("weight", 5)
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithContextList() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startObject("completion")
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .startObject("contexts")
                            .startArray("ctx")
                            .startObject()
                            .field("lat", 43.6624803)
                            .field("lon", -79.3863353)
                            .endObject()
                            .startObject()
                            .field("lat", 43.6624718)
                            .field("lon", -79.3873227)
                            .endObject()
                            .endArray()
                            .endObject()
                            .field("weight", 5)
                            .endObject()
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "loc1")
            .field("type", "geo")
            .endObject()
            .startObject()
            .field("name", "loc2")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("completion")
            .startObject()
            .array("input", "suggestion5", "suggestion6", "suggestion7")
            .field("weight", 5)
            .startObject("contexts")
            .array("loc1", "ezs42e44yx96")
            .array("loc2", "wh0n9447fwrc")
            .endObject()
            .endObject()
            .endArray()
            .endObject();
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(new SourceToParse("test", "1", BytesReference.bytes(builder), MediaTypeRegistry.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testMalformedGeoField() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("properties");
        mapping.startObject("pin");
        String type = randomFrom("text", "keyword", "long");
        mapping.field("type", type);
        mapping.endObject();
        mapping.startObject("suggestion");
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startArray("contexts");
        mapping.startObject();
        mapping.field("name", "st");
        mapping.field("type", "geo");
        mapping.field("path", "pin");
        mapping.field("precision", 5);
        mapping.endObject();
        mapping.endArray();

        mapping.endObject();

        mapping.endObject();
        mapping.endObject();

        OpenSearchParseException ex = expectThrows(OpenSearchParseException.class, () -> createIndex("test", Settings.EMPTY, mapping));

        assertThat(ex.getMessage(), equalTo("field [pin] referenced in context [st] must be mapped to geo_point, found [" + type + "]"));
    }

    public void testMissingGeoField() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("properties");
        mapping.startObject("suggestion");
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startArray("contexts");
        mapping.startObject();
        mapping.field("name", "st");
        mapping.field("type", "geo");
        mapping.field("path", "pin");
        mapping.field("precision", 5);
        mapping.endObject();
        mapping.endArray();

        mapping.endObject();

        mapping.endObject();
        mapping.endObject();

        OpenSearchParseException ex = expectThrows(OpenSearchParseException.class, () -> createIndex("test", Settings.EMPTY, mapping));

        assertThat(ex.getMessage(), equalTo("field [pin] referenced in context [st] is not defined in the mapping"));
    }

    public void testParsingQueryContextBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("ezs42e44yx96");
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("ezs42e");
        addNeighborsAtLevel("ezs42e", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, is(in(locations)));
            assertThat(internalQueryContext.boost, equalTo(1));
            assertThat(internalQueryContext.isPrefix, equalTo(false));
        }
    }

    public void testParsingQueryContextGeoPoint() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 23.654242).field("lon", 90.047153).endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        addNeighborsAtLevel("wh0n94", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, is(in(locations)));
            assertThat(internalQueryContext.boost, equalTo(1));
            assertThat(internalQueryContext.isPrefix, equalTo(false));
        }
    }

    public void testParsingQueryContextObject() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2, 3)
            .endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        locations.add("w");
        addNeighborsAtLevel("w", 1, locations);
        locations.add("wh");
        addNeighborsAtLevel("wh", 2, locations);
        locations.add("wh0");
        addNeighborsAtLevel("wh0", 3, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, is(in(locations)));
            assertThat(internalQueryContext.boost, equalTo(10));
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
        }
    }

    public void testParsingQueryContextObjectArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
            .startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2, 3)
            .endObject()
            .startObject()
            .startObject("context")
            .field("lat", 22.337374)
            .field("lon", 92.112583)
            .endObject()
            .field("boost", 2)
            .array("neighbours", 5)
            .endObject()
            .endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8 + 1 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighborsAtLevel("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighborsAtLevel("wh", 2, firstLocations);
        firstLocations.add("wh0");
        addNeighborsAtLevel("wh0", 3, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        secondLocations.add("w5cx0");
        addNeighborsAtLevel("w5cx0", 5, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(2));
            } else {
                fail(internalQueryContext.context + " was not expected");
            }
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
        }
    }

    public void testParsingQueryContextMixed() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
            .startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2)
            .endObject()
            .startObject()
            .field("lat", 22.337374)
            .field("lon", 92.112583)
            .endObject()
            .endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighborsAtLevel("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighborsAtLevel("wh", 2, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        addNeighborsAtLevel("w5cx04", 6, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(1));
            } else {
                fail(internalQueryContext.context + " was not expected");
            }
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
        }
    }
}
