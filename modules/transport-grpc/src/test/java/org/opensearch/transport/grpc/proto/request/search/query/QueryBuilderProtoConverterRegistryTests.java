/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.ChildScoreMode;
import org.opensearch.protobufs.ConstantScoreQuery;
import org.opensearch.protobufs.CoordsGeoBounds;
import org.opensearch.protobufs.DoubleArray;
import org.opensearch.protobufs.ExistsQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.FieldValueArray;
import org.opensearch.protobufs.FunctionScoreQuery;
import org.opensearch.protobufs.Fuzziness;
import org.opensearch.protobufs.FuzzyQuery;
import org.opensearch.protobufs.GeoBoundingBoxQuery;
import org.opensearch.protobufs.GeoBounds;
import org.opensearch.protobufs.GeoDistanceQuery;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.IdsQuery;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.LatLonGeoLocation;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchBoolPrefixQuery;
import org.opensearch.protobufs.MatchPhrasePrefixQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.MatchQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.NestedQuery;
import org.opensearch.protobufs.NumberRangeQuery;
import org.opensearch.protobufs.NumberRangeQueryAllOfFrom;
import org.opensearch.protobufs.NumberRangeQueryAllOfTo;
import org.opensearch.protobufs.Operator;
import org.opensearch.protobufs.PrefixQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RangeQuery;
import org.opensearch.protobufs.RegexpQuery;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptQuery;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.TermsSetQuery;
import org.opensearch.protobufs.TextQueryType;
import org.opensearch.protobufs.WildcardQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;

/**
 * Test class for QueryBuilderProtoConverterRegistry to verify the map-based optimization.
 */
public class QueryBuilderProtoConverterRegistryTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testMatchAllQueryConversion() {
        // Create a MatchAll query container
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchAllQueryBuilder",
            "org.opensearch.index.query.MatchAllQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testTermQueryConversion() {
        // Create a Term query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder().setField("test_field").setValue(FieldValue.newBuilder().setString("test_value").build()).build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a TermQueryBuilder", "org.opensearch.index.query.TermQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testScriptQueryConversion() {
        // Create a Script query container with inline script
        Script script = Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value > 100").build()).build();

        ScriptQuery scriptQuery = ScriptQuery.newBuilder().setScript(script).setBoost(1.5f).setXName("test_script_query").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setScript(scriptQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a ScriptQueryBuilder", "org.opensearch.index.query.ScriptQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testNestedQueryConversion() {
        // Create a Term query as inner query for the nested query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("user.name")
            .setValue(FieldValue.newBuilder().setString("john").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create a Nested query container
        NestedQuery nestedQuery = NestedQuery.newBuilder()
            .setPath("user")
            .setQuery(innerQueryContainer)
            .setScoreMode(ChildScoreMode.CHILD_SCORE_MODE_AVG)
            .setBoost(1.5f)
            .setXName("test_nested_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setNested(nestedQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a NestedQueryBuilder", "org.opensearch.index.query.NestedQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testNullQueryContainer() {
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(null));
    }

    public void testUnsupportedQueryType() {
        // Create an empty query container (no query type set)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));
    }

    public void testConverterRegistration() {
        // Create a custom converter for testing
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.MATCH_ALL;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                // Return a mock QueryBuilder for testing
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        // Register the custom converter
        registry.registerConverter(customConverter);

        // Test that it works
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        QueryBuilder result = registry.fromProto(queryContainer);
        assertNotNull("Result should not be null", result);
    }

    public void testNullConverter() {
        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(null));
    }

    public void testNullHandledQueryCase() {
        // Create a custom converter that returns null for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return null;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }

    public void testNotSetHandledQueryCase() {
        // Create a custom converter that returns QUERYCONTAINER_NOT_SET for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.QUERYCONTAINER_NOT_SET;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }

    public void testIdsQueryConversion() {
        // Create an Ids query container
        IdsQuery idsQuery = IdsQuery.newBuilder().addValues("doc1").addValues("doc2").setBoost(1.5f).setXName("test_ids_query").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setIds(idsQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be an IdsQueryBuilder", "org.opensearch.index.query.IdsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testRangeQueryConversion() {
        // Create a Range query container with NumberRangeQuery
        NumberRangeQueryAllOfFrom fromValue = NumberRangeQueryAllOfFrom.newBuilder().setDouble(10.0).build();
        NumberRangeQueryAllOfTo toValue = NumberRangeQueryAllOfTo.newBuilder().setDouble(100.0).build();

        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder().setField("age").setFrom(fromValue).setTo(toValue).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setRange(rangeQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a RangeQueryBuilder", "org.opensearch.index.query.RangeQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testTermsSetQueryConversion() {
        // Create a TermsSet query container
        TermsSetQuery termsSetQuery = TermsSetQuery.newBuilder()
            .setField("tags")
            .addTerms("urgent")
            .addTerms("important")
            .setMinimumShouldMatchField("tag_count")
            .setBoost(2.0f)
            .setXName("test_terms_set_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTermsSet(termsSetQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a TermsSetQueryBuilder",
            "org.opensearch.index.query.TermsSetQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testMultiMatchQueryConversion() {
        // Create a MultiMatch query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMultiMatch(
                MultiMatchQuery.newBuilder()
                    .setQuery("search term")
                    .addFields("title")
                    .addFields("content")
                    .setType(TextQueryType.TEXT_QUERY_TYPE_BEST_FIELDS)
                    .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("75%").build())
                    .setBoost(2.0f)
                    .setXName("test_multimatch_query")
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MultiMatchQueryBuilder",
            "org.opensearch.index.query.MultiMatchQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testMatchPhraseQueryConversion() {
        // Create a MatchPhrase query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchPhrase(
                MatchPhraseQuery.newBuilder()
                    .setField("title")
                    .setQuery("hello world")
                    .setAnalyzer("standard")
                    .setSlop(2)
                    .setBoost(1.5f)
                    .setXName("test_matchphrase_query")
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchPhraseQueryBuilder",
            "org.opensearch.index.query.MatchPhraseQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testExistsQueryConversion() {
        // Create an Exists query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setExists(ExistsQuery.newBuilder().setField("test_field").setBoost(2.0f).setXName("test_exists").build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be an ExistsQueryBuilder", "org.opensearch.index.query.ExistsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testRegexpQueryConversion() {
        // Create a Regexp query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setRegexp(RegexpQuery.newBuilder().setField("test_field").setValue("test.*pattern").setBoost(1.2f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a RegexpQueryBuilder", "org.opensearch.index.query.RegexpQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testWildcardQueryConversion() {
        // Create a Wildcard query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setWildcard(WildcardQuery.newBuilder().setField("test_field").setValue("test*pattern").setBoost(0.8f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a WildcardQueryBuilder",
            "org.opensearch.index.query.WildcardQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testBoolQueryConversion() {
        // Create a Bool query container with nested queries
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setBool(
                BoolQuery.newBuilder()
                    .addMust(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
                    .addShould(
                        QueryContainer.newBuilder()
                            .setTerm(
                                TermQuery.newBuilder()
                                    .setField("status")
                                    .setValue(FieldValue.newBuilder().setString("active").build())
                                    .build()
                            )
                            .build()
                    )
                    .setBoost(1.0f)
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a BoolQueryBuilder", "org.opensearch.index.query.BoolQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testGeoDistanceQueryConversion() {
        // Create a GeoDistance query container
        LatLonGeoLocation latLonLocation = LatLonGeoLocation.newBuilder().setLat(40.7589).setLon(-73.9851).build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setLatlon(latLonLocation).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("10km")
            .putLocation("location", geoLocation)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoDistance(geoDistanceQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoDistanceQueryBuilder",
            "org.opensearch.index.query.GeoDistanceQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testGeoBoundingBoxQueryConversion() {
        // Create a GeoBoundingBox query container
        CoordsGeoBounds coords = CoordsGeoBounds.newBuilder().setTop(40.7).setLeft(-74.0).setBottom(40.6).setRight(-73.9).build();

        GeoBounds geoBounds = GeoBounds.newBuilder().setCoords(coords).build();

        GeoBoundingBoxQuery geoBoundingBoxQuery = GeoBoundingBoxQuery.newBuilder().putBoundingBox("location", geoBounds).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoBoundingBox(geoBoundingBoxQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoBoundingBoxQueryBuilder",
            "org.opensearch.index.query.GeoBoundingBoxQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testGeoDistanceQueryConversionWithDoubleArray() {
        // Create a GeoDistance query with DoubleArray format
        DoubleArray doubleArray = DoubleArray.newBuilder()
            .addDoubleArray(-73.9851) // lon
            .addDoubleArray(40.7589)  // lat
            .build();

        GeoLocation geoLocation = GeoLocation.newBuilder().setDoubleArray(doubleArray).build();

        GeoDistanceQuery geoDistanceQuery = GeoDistanceQuery.newBuilder()
            .setXName("location")
            .setDistance("5mi")
            .putLocation("location", geoLocation)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setGeoDistance(geoDistanceQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a GeoDistanceQueryBuilder",
            "org.opensearch.index.query.GeoDistanceQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testRegisterConverter() {
        // Create a custom converter for testing
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public void setRegistry(org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry registry) {
                // No-op for test
            }

            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.MATCH_ALL; // Use existing case for simplicity
            }

            @Override
            public org.opensearch.index.query.QueryBuilder fromProto(QueryContainer queryContainer) {
                // Return a simple match all query for testing
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        // Test that registerConverter method can be called without throwing exceptions
        try {
            registry.registerConverter(customConverter);
            // If we reach here, no exception was thrown
        } catch (Exception e) {
            fail("registerConverter should not throw an exception: " + e.getMessage());
        }

        // Verify that the registry still works after adding a converter
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        QueryBuilder queryBuilder = registry.fromProto(queryContainer);
        assertNotNull("QueryBuilder should not be null after registering custom converter", queryBuilder);
    }

    public void testUpdateRegistryOnAllConverters() {
        // Test that updateRegistryOnAllConverters method can be called without throwing exceptions
        try {
            registry.updateRegistryOnAllConverters();
            // If we reach here, no exception was thrown
        } catch (Exception e) {
            fail("updateRegistryOnAllConverters should not throw an exception: " + e.getMessage());
        }

        // Verify that the registry still works after updating all converters
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder().setField("test_field").setValue(FieldValue.newBuilder().setString("test_value").build()).build()
            )
            .build();

        QueryBuilder queryBuilder = registry.fromProto(queryContainer);
        assertNotNull("QueryBuilder should not be null after updating registry on all converters", queryBuilder);
        assertEquals("Should be a TermQueryBuilder", "org.opensearch.index.query.TermQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testRegisterConverterAndUpdateRegistry() {
        // Test the combination of registering a converter and then updating the registry
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            private org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry registryRef;

            @Override
            public void setRegistry(org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry registry) {
                this.registryRef = registry;
            }

            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.EXISTS; // Use existing case
            }

            @Override
            public org.opensearch.index.query.QueryBuilder fromProto(QueryContainer queryContainer) {
                // Return an exists query for testing
                return new org.opensearch.index.query.ExistsQueryBuilder("test_field");
            }
        };

        // Register the custom converter
        try {
            registry.registerConverter(customConverter);
            // If we reach here, no exception was thrown
        } catch (Exception e) {
            fail("registerConverter should not throw an exception: " + e.getMessage());
        }

        // Update registry on all converters
        try {
            registry.updateRegistryOnAllConverters();
            // If we reach here, no exception was thrown
        } catch (Exception e) {
            fail("updateRegistryOnAllConverters should not throw an exception: " + e.getMessage());
        }

        // Verify that the registry still works after both operations
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setExists(ExistsQuery.newBuilder().setField("test_field").build())
            .build();

        QueryBuilder queryBuilder = registry.fromProto(queryContainer);
        assertNotNull("QueryBuilder should not be null after register and update operations", queryBuilder);
        assertEquals("Should be an ExistsQueryBuilder", "org.opensearch.index.query.ExistsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testMatchNoneQueryConversion() {
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchNone(org.opensearch.protobufs.MatchNoneQuery.newBuilder().build())
            .build();

        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchNoneQueryBuilder",
            "org.opensearch.index.query.MatchNoneQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testTermsQueryConversion() {
        // Create a Terms query container
        FieldValue fv1 = FieldValue.newBuilder().setString("electronics").build();
        FieldValue fv2 = FieldValue.newBuilder().setString("books").build();
        FieldValueArray fva = FieldValueArray.newBuilder().addFieldValueArray(fv1).addFieldValueArray(fv2).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();

        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerms(org.opensearch.protobufs.TermsQuery.newBuilder().putTerms("category", termsQueryField).setBoost(1.5f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a TermsQueryBuilder", "org.opensearch.index.query.TermsQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testConstantScoreQueryConversion() {
        // Create a ConstantScore query container with a term query inside
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setConstantScore(ConstantScoreQuery.newBuilder().setFilter(innerQueryContainer).setBoost(2.0f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a ConstantScoreQueryBuilder",
            "org.opensearch.index.query.ConstantScoreQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testFuzzyQueryConversion() {
        // Create a Fuzzy query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setFuzzy(
                FuzzyQuery.newBuilder()
                    .setField("title")
                    .setValue(FieldValue.newBuilder().setString("opensearch").build())
                    .setFuzziness(Fuzziness.newBuilder().setString("AUTO").build())
                    .setBoost(1.2f)
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a FuzzyQueryBuilder", "org.opensearch.index.query.FuzzyQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testPrefixQueryConversion() {
        // Create a Prefix query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setPrefix(PrefixQuery.newBuilder().setField("name").setValue("john").setBoost(1.1f).build())
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a PrefixQueryBuilder", "org.opensearch.index.query.PrefixQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testMatchQueryConversion() {
        // Create a Match query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatch(
                MatchQuery.newBuilder()
                    .setField("message")
                    .setQuery(FieldValue.newBuilder().setString("hello world").build())
                    .setOperator(Operator.OPERATOR_AND)
                    .setBoost(1.5f)
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a MatchQueryBuilder", "org.opensearch.index.query.MatchQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testMatchBoolPrefixQueryConversion() {
        // Create a MatchBoolPrefix query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchBoolPrefix(
                MatchBoolPrefixQuery.newBuilder()
                    .setField("title")
                    .setQuery("opensearch tutorial")
                    .setOperator(Operator.OPERATOR_OR)
                    .setBoost(1.3f)
                    .build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchBoolPrefixQueryBuilder",
            "org.opensearch.index.query.MatchBoolPrefixQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testMatchPhrasePrefixQueryConversion() {
        // Create a MatchPhrasePrefix query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchPhrasePrefix(
                MatchPhrasePrefixQuery.newBuilder().setField("title").setQuery("opensearch tuto").setSlop(2).setBoost(1.4f).build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchPhrasePrefixQueryBuilder",
            "org.opensearch.index.query.MatchPhrasePrefixQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testFunctionScoreQueryConversion() {
        // Create a FunctionScore query container with a base MatchAll query
        QueryContainer baseQuery = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder()
            .setQuery(baseQuery)
            .setBoost(1.5f)
            .setXName("test_function_score_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setFunctionScore(functionScoreQuery).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a FunctionScoreQueryBuilder",
            "org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }
}
