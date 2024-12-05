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

import org.apache.lucene.analysis.TokenStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentContraints;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisMode;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.ReloadableCustomAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.InvalidTypeNameException;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MapperServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, ReloadableFilterPlugin.class);
    }

    public void testTypeValidation() {
        InvalidTypeNameException e = expectThrows(InvalidTypeNameException.class, () -> MapperService.validateTypeName("_type"));
        assertEquals("mapping type name [_type] can't start with '_' unless it is called [_doc]", e.getMessage());

        e = expectThrows(InvalidTypeNameException.class, () -> MapperService.validateTypeName("_document"));
        assertEquals("mapping type name [_document] can't start with '_' unless it is called [_doc]", e.getMessage());

        MapperService.validateTypeName("_doc"); // no exception
    }

    public void testGetMetadataFieldsReturnsExpectedSet() throws Throwable {
        final MapperService mapperService = createIndex("test1").mapperService();
        assertEquals(mapperService.getMetadataFields(), IndicesModule.getBuiltInMetadataFields());
    }

    public void testPreflightUpdateDoesNotChangeMapping() throws Throwable {
        final MapperService mapperService = createIndex("test1").mapperService();
        final CompressedXContent mapping = createMappingSpecifyingNumberOfFields(1);
        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE_PREFLIGHT);
        assertThat("field was not created by preflight check", mapperService.fieldType("field0"), nullValue());
        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);
        assertThat("field was not created by mapping update", mapperService.fieldType("field0"), notNullValue());
    }

    /**
     * Test that we can have at least the number of fields in new mappings that are defined by "index.mapping.total_fields.limit".
     * Any additional field should trigger an IllegalArgumentException.
     */
    public void testTotalFieldsLimit() throws Throwable {
        int totalFieldsLimit = randomIntBetween(1, 10);
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldsLimit)
            .build();
        createIndex("test1", settings).mapperService()
            .merge("type", createMappingSpecifyingNumberOfFields(totalFieldsLimit), MergeReason.MAPPING_UPDATE);

        // adding one more field should trigger exception
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test2", settings).mapperService()
                .merge("type", createMappingSpecifyingNumberOfFields(totalFieldsLimit + 1), updateOrPreflight());
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Limit of total fields [" + totalFieldsLimit + "] has been exceeded"));
    }

    private CompressedXContent createMappingSpecifyingNumberOfFields(int numberOfFields) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (int i = 0; i < numberOfFields; i++) {
            mappingBuilder.startObject("field" + i);
            mappingBuilder.field("type", randomFrom("long", "integer", "date", "keyword", "text"));
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject().endObject();
        return new CompressedXContent(BytesReference.bytes(mappingBuilder));
    }

    public void testMappingDepthExceedsLimit() throws Throwable {
        IndexService indexService1 = createIndex(
            "test1",
            Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), 1).build()
        );
        // no exception
        indexService1.mapperService().merge("type", createMappingSpecifyingNumberOfFields(1), MergeReason.MAPPING_UPDATE);

        CompressedXContent objectMapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("object1")
                    .field("type", "object")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IndexService indexService2 = createIndex("test2");
        // no exception
        indexService2.mapperService().merge("type", objectMapping, MergeReason.MAPPING_UPDATE);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indexService1.mapperService().merge("type", objectMapping, updateOrPreflight())
        );
        assertThat(e.getMessage(), containsString("Limit of mapping depth [1] has been exceeded"));
    }

    public void testMappingDepthExceedsXContentLimit() throws Throwable {
        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                "test1",
                Settings.builder()
                    .put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), XContentContraints.DEFAULT_MAX_DEPTH + 1)
                    .build()
            )
        );

        assertThat(
            ex.getMessage(),
            is(
                "The provided value 1001 of the index setting 'index.mapping.depth.limit' exceeds per-JVM configured limit of 1000. "
                    + "Please change the setting value or increase per-JVM limit using 'opensearch.xcontent.depth.max' system property."
            )
        );
    }

    public void testUnmappedFieldType() {
        MapperService mapperService = createIndex("index").mapperService();
        assertThat(mapperService.unmappedFieldType("keyword"), instanceOf(KeywordFieldType.class));
        assertThat(mapperService.unmappedFieldType("long"), instanceOf(NumberFieldType.class));
        // back compat
        assertThat(mapperService.unmappedFieldType("string"), instanceOf(KeywordFieldType.class));
        assertWarnings("[unmapped_type:string] should be replaced with [unmapped_type:keyword]");
    }

    public void testPartitionedConstraints() {
        // partitioned index must have routing
        IllegalArgumentException noRoutingException = expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .indices()
                .prepareCreate("test-index")
                .setMapping("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}")
                .setSettings(Settings.builder().put("index.number_of_shards", 4).put("index.routing_partition_size", 2))
                .execute()
                .actionGet();
        });
        assertTrue(noRoutingException.getMessage(), noRoutingException.getMessage().contains("must have routing"));

        // valid partitioned index
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate("test-index")
                .setMapping("{\"_routing\":{\"required\":true}}")
                .setSettings(Settings.builder().put("index.number_of_shards", 4).put("index.routing_partition_size", 2))
                .execute()
                .actionGet()
                .isAcknowledged()
        );
    }

    public void testIndexSortWithNestedFields() throws IOException {
        Settings settings = Settings.builder().put("index.sort.field", "foo").build();
        IllegalArgumentException invalidNestedException = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test", settings, "t", "nested_field", "type=nested", "foo", "type=keyword")
        );
        assertThat(invalidNestedException.getMessage(), containsString("cannot have nested fields when index sort is activated"));
        IndexService indexService = createIndex("test", settings, "t", "foo", "type=keyword");
        CompressedXContent nestedFieldMapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("nested_field")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        invalidNestedException = expectThrows(
            IllegalArgumentException.class,
            () -> indexService.mapperService().merge("t", nestedFieldMapping, updateOrPreflight())
        );
        assertThat(invalidNestedException.getMessage(), containsString("cannot have nested fields when index sort is activated"));
    }

    public void testFieldAliasWithMismatchedNestedScope() throws Throwable {
        IndexService indexService = createIndex("test");
        MapperService mapperService = indexService.mapperService();

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent mappingUpdate = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("alias")
                    .field("type", "alias")
                    .field("path", "nested.field")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge("type", mappingUpdate, updateOrPreflight())
        );
        assertThat(e.getMessage(), containsString("Invalid [path] value [nested.field] for field alias [alias]"));
    }

    public void testTotalFieldsLimitWithFieldAlias() throws Throwable {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("alias")
            .field("type", "alias")
            .field("path", "field")
            .endObject()
            .startObject("field")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        int numberOfFieldsIncludingAlias = 2;
        createIndex(
            "test1",
            Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfFieldsIncludingAlias).build()
        ).mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        // Set the total fields limit to the number of non-alias fields, to verify that adding
        // a field alias pushes the mapping over the limit.
        int numberOfNonAliasFields = 1;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex(
                "test2",
                Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfNonAliasFields).build()
            ).mapperService().merge("type", new CompressedXContent(mapping), updateOrPreflight());
        });
        assertEquals("Limit of total fields [" + numberOfNonAliasFields + "] has been exceeded", e.getMessage());
    }

    public void testFieldNameLengthExceedsXContentLimit() throws Throwable {
        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                "test1",
                Settings.builder()
                    .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), XContentContraints.DEFAULT_MAX_NAME_LEN + 1)
                    .build()
            )
        );

        assertThat(
            ex.getMessage(),
            is(
                "The provided value 50001 of the index setting 'index.mapping.field_name_length.limit' exceeds per-JVM configured limit of 50000. "
                    + "Please change the setting value or increase per-JVM limit using 'opensearch.xcontent.name.length.max' system property."
            )
        );
    }

    public void testFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("type")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent mappingUpdate = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(testString)
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mappingUpdate, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testObjectNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("type")
                    .startObject("properties")
                    .startObject(testString)
                    .field("type", "object")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mapping, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testAliasFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("type")
                    .startObject("properties")
                    .startObject(testString)
                    .field("type", "alias")
                    .field("path", "field")
                    .endObject()
                    .startObject("field")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mapping, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testMappingRecoverySkipFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("type")
                    .startObject("properties")
                    .startObject(testString)
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        DocumentMapper documentMapper = mapperService.merge("type", mapping, MergeReason.MAPPING_RECOVERY);

        assertEquals(testString, documentMapper.mappers().getMapper(testString).simpleName());
    }

    public void testReloadSearchAnalyzers() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.analysis.analyzer.reloadableAnalyzer.type", "custom")
            .put("index.analysis.analyzer.reloadableAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.reloadableAnalyzer.filter", "myReloadableFilter")
            .build();

        MapperService mapperService = createIndex("test_index", settings).mapperService();
        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", "text")
                    .field("analyzer", "simple")
                    .field("search_analyzer", "reloadableAnalyzer")
                    .field("search_quote_analyzer", "stop")
                    .endObject()
                    .startObject("otherField")
                    .field("type", "text")
                    .field("analyzer", "standard")
                    .field("search_analyzer", "simple")
                    .field("search_quote_analyzer", "reloadableAnalyzer")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        mapperService.merge("_doc", mapping, MergeReason.MAPPING_UPDATE);
        IndexAnalyzers current = mapperService.getIndexAnalyzers();

        ReloadableCustomAnalyzer originalReloadableAnalyzer = (ReloadableCustomAnalyzer) current.get("reloadableAnalyzer").analyzer();
        TokenFilterFactory[] originalTokenFilters = originalReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(1, originalTokenFilters.length);
        assertEquals("myReloadableFilter", originalTokenFilters[0].name());

        // now reload, this should change the tokenfilterFactory inside the analyzer
        mapperService.reloadSearchAnalyzers(getInstanceFromNode(AnalysisRegistry.class));
        IndexAnalyzers updatedAnalyzers = mapperService.getIndexAnalyzers();
        assertSame(current, updatedAnalyzers);
        assertSame(current.getDefaultIndexAnalyzer(), updatedAnalyzers.getDefaultIndexAnalyzer());
        assertSame(current.getDefaultSearchAnalyzer(), updatedAnalyzers.getDefaultSearchAnalyzer());
        assertSame(current.getDefaultSearchQuoteAnalyzer(), updatedAnalyzers.getDefaultSearchQuoteAnalyzer());

        assertFalse(assertSameContainedFilters(originalTokenFilters, current.get("reloadableAnalyzer")));
        assertFalse(
            assertSameContainedFilters(originalTokenFilters, mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer())
        );
        assertFalse(
            assertSameContainedFilters(
                originalTokenFilters,
                mapperService.fieldType("otherField").getTextSearchInfo().getSearchQuoteAnalyzer()
            )
        );
    }

    public void testMapperDynamicAllowedIgnored() {
        final List<Function<Settings.Builder, Settings.Builder>> scenarios = List.of(
            (builder) -> builder.putNull(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey()),
            (builder) -> builder.put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), true),
            (builder) -> builder.put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), false)
        );

        for (int i = 0; i < scenarios.size(); i++) {
            final Settings.Builder defaultSettingsBuilder = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);

            final Settings settings = scenarios.get(i).apply(defaultSettingsBuilder).build();

            createIndex("test" + i, settings).mapperService();
        }

        assertWarnings(
            "[index.mapper.dynamic] setting was deprecated in OpenSearch and will be removed in a future release! See the breaking changes documentation for the next major version."
        );
    }

    private boolean assertSameContainedFilters(TokenFilterFactory[] originalTokenFilter, NamedAnalyzer updatedAnalyzer) {
        ReloadableCustomAnalyzer updatedReloadableAnalyzer = (ReloadableCustomAnalyzer) updatedAnalyzer.analyzer();
        TokenFilterFactory[] newTokenFilters = updatedReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(originalTokenFilter.length, newTokenFilters.length);
        int i = 0;
        for (TokenFilterFactory tf : newTokenFilters) {
            assertEquals(originalTokenFilter[i].name(), tf.name());
            if (originalTokenFilter[i] != tf) {
                return false;
            }
            i++;
        }
        return true;
    }

    private static MergeReason updateOrPreflight() {
        return randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.MAPPING_UPDATE_PREFLIGHT);
    }

    public static final class ReloadableFilterPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("myReloadableFilter", new AnalysisProvider<TokenFilterFactory>() {

                @Override
                public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
                    throws IOException {
                    return new TokenFilterFactory() {

                        @Override
                        public String name() {
                            return "myReloadableFilter";
                        }

                        @Override
                        public TokenStream create(TokenStream tokenStream) {
                            return tokenStream;
                        }

                        @Override
                        public AnalysisMode getAnalysisMode() {
                            return AnalysisMode.SEARCH_TIME;
                        }
                    };
                }
            });
        }
    }

}
