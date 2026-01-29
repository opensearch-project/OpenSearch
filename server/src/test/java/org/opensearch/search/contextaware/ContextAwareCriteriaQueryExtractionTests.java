/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.contextaware;

import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.FilterAwareQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonMap;

/**
 * Unit tests for {@link ContextAwareCriteriaQueryExtraction}
 */
public class ContextAwareCriteriaQueryExtractionTests extends OpenSearchTestCase {

    private static final String GROUPING_FIELD_NAME = "grouping_field";

    /**
     * mapping:
     * {
     *   "properties": {
     *     "grouping_field": {
     *       "type": "text"
     *     }
     *   }
     *  }
     */
    public void testConstructorWhenMapperIsMissing() throws IOException {
        // Given
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(GROUPING_FIELD_NAME)
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        MapperService mapperService = createMapperService(mapping.toString());

        // When
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"));

        // Then
        assertEquals(Collections.emptySet(), result);
    }

    public void testExtractWithNullQuery() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());

        // When
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(null);

        // Then
        assertTrue("Result should be empty for a null query", result.isEmpty());
    }

    public void testExtractWithUnhandledQueryType() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());

        // When
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new MatchAllQueryBuilder());

        // Then
        assertTrue("Result should be empty for unhandled query types", result.isEmpty());
    }

    public void testExtractFromSimpleTermQuery() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);

        // When
        QueryBuilder query = new TermQueryBuilder(GROUPING_FIELD_NAME, "value1");
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1"), result);
    }

    public void testExtractFromTermQueryOnWrongField() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());

        // When
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new TermQueryBuilder("other_field", "value1"));

        // Then
        assertTrue("Result should be empty if TermQuery is on a different field", result.isEmpty());
    }

    public void testExtractFromSimpleTermsQuery() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);

        // When
        QueryBuilder query = new TermsQueryBuilder(GROUPING_FIELD_NAME, "value1", "value2", "value3");
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1", "value2", "value3"), result);
    }

    public void testExtractFromTermsQueryOnWrongField() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());

        // When
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new TermsQueryBuilder("other_field", "value1", "value2"));

        // Then
        assertTrue("Result should be empty if TermsQuery is on a different field", result.isEmpty());
    }

    public void testExtractFromTermsQueryWithScript() throws IOException {
        // Given
        Function<Map<String, Object>, Object> scriptLogic = params -> "transformed_" + params.get(GROUPING_FIELD_NAME);
        MapperService mapperService = createMapperService(getMappingWithScript(), scriptLogic);
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);

        // When
        QueryBuilder query = new TermsQueryBuilder(GROUPING_FIELD_NAME, "value1", "value2");
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("transformed_value1", "transformed_value2"), result);
    }

    public void testExtractFromWithFilterQuery() throws IOException {
        // Given
        QueryBuilder filter = new TermQueryBuilder(GROUPING_FIELD_NAME, "value1");
        class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> implements FilterAwareQueryBuilder {

            @Override
            protected void doWriteTo(StreamOutput out) throws IOException {}

            @Override
            protected void doXContent(XContentBuilder builder, Params params) throws IOException {}

            @Override
            protected Query doToQuery(QueryShardContext context) throws IOException {
                return null;
            }

            @Override
            protected boolean doEquals(TestQueryBuilder other) {
                return false;
            }

            @Override
            protected int doHashCode() {
                return 0;
            }

            @Override
            public String getWriteableName() {
                return "";
            }

            @Override
            public QueryBuilder filterQueryBuilder() {
                return filter;
            }
        }

        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        QueryBuilder query = new TestQueryBuilder();

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1"), result);
    }

    public void testBoolQueryWithSingleFilter() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().filter(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"));

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1"), result);
    }

    public void testBoolQueryWithMultipleFiltersReturnsEmpty() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().filter(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"))
            .filter(new TermQueryBuilder(GROUPING_FIELD_NAME, "value2"));

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertTrue("Result should be empty with more than one filter clause containing criteria", result.isEmpty());
    }

    public void testBoolQueryWithSingleMust() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"));

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1"), result);
    }

    public void testBoolQueryWithMultipleConsistentMustClauses() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"))
            .must(new MatchAllQueryBuilder()) // This one has no criteria, which is allowed
            .must(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"));

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1"), result);
    }

    public void testBoolQueryWithConflictingMustClausesReturnsEmpty() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"))
            .must(new TermQueryBuilder(GROUPING_FIELD_NAME, "value2"));

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertTrue("Result should be empty with conflicting must clauses", result.isEmpty());
    }

    public void testBoolQueryWithShouldClauses() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().should(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"))
            .should(new TermQueryBuilder(GROUPING_FIELD_NAME, "value2"))
            .minimumShouldMatch(1);

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(Set.of("value1", "value2"), result);
    }

    public void testBoolQueryWithInvalidShouldClauseReturnsEmpty() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().should(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"))
            .should(new MatchAllQueryBuilder()) // This clause has no criteria, which is invalid for should
            .minimumShouldMatch(1);

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertTrue("Result should be empty if a should clause lacks criteria", result.isEmpty());
    }

    public void testBoolQueryFilterTakesPrecedenceOverMust() throws IOException {
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        BoolQueryBuilder query = new BoolQueryBuilder().filter(new TermQueryBuilder(GROUPING_FIELD_NAME, "filter_value"))
            .must(new TermQueryBuilder(GROUPING_FIELD_NAME, "must_value"));
        Set<String> result = extractor.extractCriteria(query);
        assertEquals("Filter clause should take precedence", Set.of("filter_value"), result);
    }

    public void testNestedBoolQueryWithFilterPrecedence() throws IOException {
        // Give
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);

        BoolQueryBuilder query = new BoolQueryBuilder().filter(
            new BoolQueryBuilder().must(new TermQueryBuilder(GROUPING_FIELD_NAME, "correct_value"))
                .should(new TermQueryBuilder("other_field", "some_other_value"))
        )
            .must(new TermQueryBuilder(GROUPING_FIELD_NAME, "ignored_must_value"))
            .should(new TermQueryBuilder(GROUPING_FIELD_NAME, "ignored_should_value"))
            .minimumShouldMatch(1);

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals("Should extract criteria from nested bool in filter, ignoring outer clauses", Set.of("correct_value"), result);
    }

    public void testNestedBoolQueryWithShouldInsideMust() throws IOException {
        // Given
        MapperService mapperService = createMapperService(getKeywordMapping());
        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);

        BoolQueryBuilder query = new BoolQueryBuilder().must(
            new BoolQueryBuilder().should(new TermQueryBuilder(GROUPING_FIELD_NAME, "correct_value"))
                .should(new TermQueryBuilder(GROUPING_FIELD_NAME, "some_other_value"))
        ).should(new TermQueryBuilder(GROUPING_FIELD_NAME, "ignored_should_value")).minimumShouldMatch(1);

        // When
        Set<String> result = extractor.extractCriteria(query);

        // Then
        assertEquals(
            "Should extract criteria from nested bool in filter, ignoring outer clauses",
            Set.of("correct_value", "some_other_value"),
            result
        );
    }

    public void testExtractWithValueScript() throws IOException {
        Function<Map<String, Object>, Object> scriptLogic = params -> "transformed_" + params.get(GROUPING_FIELD_NAME);
        MapperService mapperService = createMapperService(getMappingWithScript(), scriptLogic);

        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new TermQueryBuilder(GROUPING_FIELD_NAME, "value1"));
        assertEquals(Set.of("transformed_value1"), result);
    }

    public void testExtractWithNumericFieldAndScript() throws IOException {
        // Script will receive a Long and transform it to a string
        Function<Map<String, Object>, Object> scriptLogic = params -> {
            Long val = Long.valueOf(params.get(GROUPING_FIELD_NAME).toString());
            return "numeric_" + (val + 10); // e.g., 123 -> "numeric_133"
        };

        // The mapping is almost the same as the script mapping, but the field type is "long"
        String numericScriptMapping = getMappingWithScript().replace("\"text\"", "\"long\"");
        MapperService mapperService = createMapperService(numericScriptMapping, scriptLogic);

        ContextAwareCriteriaQueryExtraction extractor = new ContextAwareCriteriaQueryExtraction(mapperService);
        Set<String> result = extractor.extractCriteria(new TermQueryBuilder(GROUPING_FIELD_NAME, "123"));
        assertEquals("Numeric value should be parsed before script execution", Set.of("numeric_133"), result);
    }

    /**
     * Provides a mapping that includes a script definition.
     */
    private String getMappingWithScript() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("context_aware_grouping")
            .array("fields", GROUPING_FIELD_NAME)
            .startObject("script")
            .field("source", "my_test_script")
            .field("lang", "painless")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject(GROUPING_FIELD_NAME)
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();

        return mapping.toString();
    }

    /**
     * mapping:
     * {
     *   "context_aware_grouping":{
     *     "fields": ["grouping_field"]
     *   },
     *   "properties": {
     *     "grouping_field": {
     *       "type": "text"
     *     }
     *   }
     *  }
     */
    private String getKeywordMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("context_aware_grouping")
            .array("fields", GROUPING_FIELD_NAME)
            .endObject()
            .startObject("properties")
            .startObject(GROUPING_FIELD_NAME)
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        return mapping.toString();
    }

    private static MapperService createMapperService(final String mapping) throws IOException {
        return createMapperService(mapping, null);
    }

    private static MapperService createMapperService(final String mapping, final Function<Map<String, Object>, Object> scriptTransform)
        throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(mapping)
            .build();
        MapperService mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            "test",
            createScriptService(scriptTransform)
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    private static ScriptService createScriptService(final Function<Map<String, Object>, Object> scriptTransform) {
        // A mock script engine that we can control for the test
        MockScriptEngine scriptEngine = new MockScriptEngine(
            "painless",
            singletonMap("my_test_script", scriptTransform), // Script name and its behavior
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
