/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.FieldValueFactorModifier;
import org.opensearch.protobufs.FieldValueFactorScoreFunction;
import org.opensearch.protobufs.FunctionBoostMode;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.FunctionScoreMode;
import org.opensearch.protobufs.FunctionScoreQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RandomScoreFunction;
import org.opensearch.protobufs.RandomScoreFunctionSeed;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class FunctionScoreQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Initialize the registry for FunctionScoreQueryBuilderProtoUtils
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithBasicFunctionScoreQuery() {
        // Create a basic function score query with a term query
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setValue(FieldValue.newBuilder().setString("test_value").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder()
            .setQuery(queryContainer)
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_MULTIPLY)
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_MULTIPLY)
            .setMaxBoost(10.0f)
            .setMinScore(0.5f)
            .build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        assertEquals(10.0f, functionScoreQueryBuilder.maxBoost(), 0.001f);
        assertEquals(0.5f, functionScoreQueryBuilder.getMinScore(), 0.001f);
    }

    public void testFromProtoWithFieldValueFactorFunction() {
        // Create a function score query with field_value_factor function
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("popularity")
            .setFactor(1.2f)
            .setMissing(1.0f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setWeight(2.0f)
            .build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);
    }

    public void testFromProtoWithRandomScoreFunction() {
        // Create a function score query with random_score function
        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder()
            .setField("_id")
            .setSeed(RandomScoreFunctionSeed.newBuilder().setInt32(42).build())
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).setWeight(1.5f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);
    }

    public void testFromProtoWithNullInput() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionScoreQueryBuilderProtoUtils.fromProto(null, registry)
        );

        assertThat(exception.getMessage(), containsString("FunctionScoreQuery cannot be null"));
    }

    public void testFromProtoWithEmptyQueryAndNoFunctions() {
        // Create a function score query with neither query nor functions
        // This should default to MatchAllQueryBuilder (matching fromXContent behavior)
        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Should have a MatchAllQueryBuilder as the base query
        assertThat(functionScoreQueryBuilder.query(), instanceOf(MatchAllQueryBuilder.class));
        // Should have no functions
        assertEquals(0, functionScoreQueryBuilder.filterFunctionBuilders().length);
    }

    public void testFromProtoWithWeightOnlyFunction() {
        // Create a function score query with only weight (no specific function)
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setWeight(3.0f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);
    }

    public void testFromProtoWithScriptScoreFunction() {
        // Create an inline script
        org.opensearch.protobufs.InlineScript inlineScript = org.opensearch.protobufs.InlineScript.newBuilder()
            .setSource("Math.log(doc['popularity'].value) * params.multiplier")
            .setLang(
                org.opensearch.protobufs.ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .setParams(
                org.opensearch.protobufs.ObjectMap.newBuilder()
                    .putFields("multiplier", org.opensearch.protobufs.ObjectMap.Value.newBuilder().setDouble(2.0).build())
                    .build()
            )
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        org.opensearch.protobufs.ScriptScoreFunction scriptScoreFunction = org.opensearch.protobufs.ScriptScoreFunction.newBuilder()
            .setScript(script)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).setWeight(1.5f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the script score function
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertThat(
            filterFunction.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder.class)
        );

        org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder scriptFunction =
            (org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder) filterFunction.getScoreFunction();

        // Verify script properties
        org.opensearch.script.Script openSearchScript = scriptFunction.getScript();
        assertEquals(org.opensearch.script.ScriptType.INLINE, openSearchScript.getType());
        assertEquals("painless", openSearchScript.getLang());
        assertEquals("Math.log(doc['popularity'].value) * params.multiplier", openSearchScript.getIdOrCode());
        assertEquals(2.0, openSearchScript.getParams().get("multiplier"));
    }

    public void testFromProtoWithExponentialDecayFunction() {
        // Create a numeric decay placement
        org.opensearch.protobufs.NumericDecayPlacement numericPlacement = org.opensearch.protobufs.NumericDecayPlacement.newBuilder()
            .setOrigin(10.0)
            .setScale(5.0)
            .setOffset(1.0)
            .setDecay(0.3)
            .build();

        // Create a decay placement
        org.opensearch.protobufs.DecayPlacement decayPlacement = org.opensearch.protobufs.DecayPlacement.newBuilder()
            .setNumericDecayPlacement(numericPlacement)
            .build();

        // Create a decay function with the placement
        org.opensearch.protobufs.DecayFunction decayFunction = org.opensearch.protobufs.DecayFunction.newBuilder()
            .putPlacement("price", decayPlacement)
            .build();

        // Create a function score container with the exponential decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.5f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the exponential decay function
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertThat(
            filterFunction.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder.class)
        );

        org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder expFunction =
            (org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder) filterFunction.getScoreFunction();

        // Verify the field name
        assertEquals("price", expFunction.getFieldName());
    }

    public void testFromProtoWithGaussianDecayFunction() {
        // Create a numeric decay placement
        org.opensearch.protobufs.NumericDecayPlacement numericPlacement = org.opensearch.protobufs.NumericDecayPlacement.newBuilder()
            .setOrigin(20.0)
            .setScale(10.0)
            .setOffset(2.0)
            .setDecay(0.4)
            .build();

        // Create a decay placement
        org.opensearch.protobufs.DecayPlacement decayPlacement = org.opensearch.protobufs.DecayPlacement.newBuilder()
            .setNumericDecayPlacement(numericPlacement)
            .build();

        // Create a decay function with the placement
        org.opensearch.protobufs.DecayFunction decayFunction = org.opensearch.protobufs.DecayFunction.newBuilder()
            .putPlacement("rating", decayPlacement)
            .build();

        // Create a function score container with the gaussian decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(2.0f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the gaussian decay function
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertThat(filterFunction.getScoreFunction(), instanceOf(org.opensearch.index.query.functionscore.GaussDecayFunctionBuilder.class));

        org.opensearch.index.query.functionscore.GaussDecayFunctionBuilder gaussFunction =
            (org.opensearch.index.query.functionscore.GaussDecayFunctionBuilder) filterFunction.getScoreFunction();

        // Verify the field name
        assertEquals("rating", gaussFunction.getFieldName());
    }

    public void testFromProtoWithLinearDecayFunction() {
        // Create a numeric decay placement
        org.opensearch.protobufs.NumericDecayPlacement numericPlacement = org.opensearch.protobufs.NumericDecayPlacement.newBuilder()
            .setOrigin(100.0)
            .setScale(50.0)
            .setOffset(5.0)
            .setDecay(0.2)
            .build();

        // Create a decay placement
        org.opensearch.protobufs.DecayPlacement decayPlacement = org.opensearch.protobufs.DecayPlacement.newBuilder()
            .setNumericDecayPlacement(numericPlacement)
            .build();

        // Create a decay function with the placement
        org.opensearch.protobufs.DecayFunction decayFunction = org.opensearch.protobufs.DecayFunction.newBuilder()
            .putPlacement("distance", decayPlacement)
            .build();

        // Create a function score container with the linear decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.0f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the linear decay function
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertThat(
            filterFunction.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder.class)
        );

        org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder linearFunction =
            (org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder) filterFunction.getScoreFunction();

        // Verify the field name
        assertEquals("distance", linearFunction.getFieldName());
    }

    public void testFromProtoWithMultipleFunctions() {
        // Create first function - field value factor
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("popularity")
            .setFactor(1.5f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        FunctionScoreContainer container1 = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setWeight(2.0f)
            .build();

        // Create second function - random score
        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder()
            .setField("_id")
            .setSeed(RandomScoreFunctionSeed.newBuilder().setString("test_seed").build())
            .build();

        FunctionScoreContainer container2 = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).setWeight(1.0f).build();

        // Create third function - exponential decay
        org.opensearch.protobufs.NumericDecayPlacement numericPlacement = org.opensearch.protobufs.NumericDecayPlacement.newBuilder()
            .setOrigin(15.0)
            .setScale(8.0)
            .setDecay(0.3)
            .build();

        org.opensearch.protobufs.DecayPlacement decayPlacement = org.opensearch.protobufs.DecayPlacement.newBuilder()
            .setNumericDecayPlacement(numericPlacement)
            .build();

        org.opensearch.protobufs.DecayFunction decayFunction = org.opensearch.protobufs.DecayFunction.newBuilder()
            .putPlacement("price", decayPlacement)
            .build();

        FunctionScoreContainer container3 = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.5f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder()
            .addFunctions(container1)
            .addFunctions(container2)
            .addFunctions(container3)
            .build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that all three functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(3, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the first function (field value factor)
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction1 = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertThat(
            filterFunction1.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder.class)
        );

        // Verify the second function (random score)
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction2 = functionScoreQueryBuilder.filterFunctionBuilders()[1];
        assertThat(
            filterFunction2.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder.class)
        );

        // Verify the third function (exponential decay)
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction3 = functionScoreQueryBuilder.filterFunctionBuilders()[2];
        assertThat(
            filterFunction3.getScoreFunction(),
            instanceOf(org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder.class)
        );
    }

    public void testFromProtoWithDifferentBoostModes() {
        // Test MULTIPLY boost mode
        FunctionScoreQuery functionScoreQuery1 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_MULTIPLY)
            .build();

        QueryBuilder result1 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery1, registry);
        assertThat(result1, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder1 = (FunctionScoreQueryBuilder) result1;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.MULTIPLY, builder1.boostMode());

        // Test REPLACE boost mode
        FunctionScoreQuery functionScoreQuery2 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_REPLACE)
            .build();

        QueryBuilder result2 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery2, registry);
        assertThat(result2, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder2 = (FunctionScoreQueryBuilder) result2;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.REPLACE, builder2.boostMode());

        // Test SUM boost mode
        FunctionScoreQuery functionScoreQuery3 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_SUM)
            .build();

        QueryBuilder result3 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery3, registry);
        assertThat(result3, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder3 = (FunctionScoreQueryBuilder) result3;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.SUM, builder3.boostMode());

        // Test AVG boost mode
        FunctionScoreQuery functionScoreQuery4 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_AVG)
            .build();

        QueryBuilder result4 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery4, registry);
        assertThat(result4, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder4 = (FunctionScoreQueryBuilder) result4;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.AVG, builder4.boostMode());

        // Test MAX boost mode
        FunctionScoreQuery functionScoreQuery5 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_MAX)
            .build();

        QueryBuilder result5 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery5, registry);
        assertThat(result5, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder5 = (FunctionScoreQueryBuilder) result5;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.MAX, builder5.boostMode());

        // Test MIN boost mode
        FunctionScoreQuery functionScoreQuery6 = FunctionScoreQuery.newBuilder()
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_MIN)
            .build();

        QueryBuilder result6 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery6, registry);
        assertThat(result6, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder6 = (FunctionScoreQueryBuilder) result6;
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.MIN, builder6.boostMode());
    }

    public void testFromProtoWithDifferentScoreModes() {
        // Test MULTIPLY score mode
        FunctionScoreQuery functionScoreQuery1 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_MULTIPLY)
            .build();

        QueryBuilder result1 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery1, registry);
        assertThat(result1, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder1 = (FunctionScoreQueryBuilder) result1;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MULTIPLY, builder1.scoreMode());

        // Test SUM score mode
        FunctionScoreQuery functionScoreQuery2 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_SUM)
            .build();

        QueryBuilder result2 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery2, registry);
        assertThat(result2, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder2 = (FunctionScoreQueryBuilder) result2;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.SUM, builder2.scoreMode());

        // Test AVG score mode
        FunctionScoreQuery functionScoreQuery3 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_AVG)
            .build();

        QueryBuilder result3 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery3, registry);
        assertThat(result3, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder3 = (FunctionScoreQueryBuilder) result3;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.AVG, builder3.scoreMode());

        // Test FIRST score mode
        FunctionScoreQuery functionScoreQuery4 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_FIRST)
            .build();

        QueryBuilder result4 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery4, registry);
        assertThat(result4, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder4 = (FunctionScoreQueryBuilder) result4;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.FIRST, builder4.scoreMode());

        // Test MAX score mode
        FunctionScoreQuery functionScoreQuery5 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_MAX)
            .build();

        QueryBuilder result5 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery5, registry);
        assertThat(result5, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder5 = (FunctionScoreQueryBuilder) result5;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MAX, builder5.scoreMode());

        // Test MIN score mode
        FunctionScoreQuery functionScoreQuery6 = FunctionScoreQuery.newBuilder()
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_MIN)
            .build();

        QueryBuilder result6 = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery6, registry);
        assertThat(result6, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder builder6 = (FunctionScoreQueryBuilder) result6;
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MIN, builder6.scoreMode());
    }

    public void testFromProtoWithFunctionAndFilter() {
        // Create a term query for the filter
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("category")
            .setValue(FieldValue.newBuilder().setString("electronics").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create a field value factor function
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("rating")
            .setFactor(2.0f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQRT)
            .build();

        // Create a function score container with both function and filter
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setFilter(queryContainer)
            .setWeight(1.5f)
            .build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify that functions were added
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(1, functionScoreQueryBuilder.filterFunctionBuilders().length);

        // Verify the function has a filter
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunction = functionScoreQueryBuilder.filterFunctionBuilders()[0];
        assertNotNull(filterFunction.getFilter());
        assertThat(filterFunction.getFilter(), instanceOf(org.opensearch.index.query.TermQueryBuilder.class));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        // Create an empty decay function (no placements)
        org.opensearch.protobufs.DecayFunction decayFunction = org.opensearch.protobufs.DecayFunction.newBuilder().build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.0f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().addFunctions(container).build();

        // This should throw an exception because decay function has no placements
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithAllParameters() {
        // Create a comprehensive function score query with all parameters
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("title")
            .setValue(FieldValue.newBuilder().setString("search").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Create multiple functions
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("popularity")
            .setFactor(1.2f)
            .setMissing(1.0f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        FunctionScoreContainer container1 = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setWeight(2.0f)
            .build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder()
            .setField("_id")
            // .setSeed(RandomScoreFunctionSeed.newBuilder().setInt32Value(123).build()) // Method not available in 0.16.0
            .build();

        FunctionScoreContainer container2 = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).setWeight(0.5f).build();

        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder()
            .setQuery(queryContainer)
            .setBoostMode(FunctionBoostMode.FUNCTION_BOOST_MODE_MULTIPLY)
            .setScoreMode(FunctionScoreMode.FUNCTION_SCORE_MODE_SUM)
            .setMaxBoost(15.0f)
            .setMinScore(0.1f)
            .setBoost(2.0f)
            .addFunctions(container1)
            .addFunctions(container2)
            .build();

        QueryBuilder result = FunctionScoreQueryBuilderProtoUtils.fromProto(functionScoreQuery, registry);

        assertThat(result, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) result;

        // Verify all parameters
        assertThat(functionScoreQueryBuilder.query(), instanceOf(org.opensearch.index.query.TermQueryBuilder.class));
        assertEquals(org.opensearch.common.lucene.search.function.CombineFunction.MULTIPLY, functionScoreQueryBuilder.boostMode());
        assertEquals(org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.SUM, functionScoreQueryBuilder.scoreMode());
        assertEquals(15.0f, functionScoreQueryBuilder.maxBoost(), 0.001f);
        assertEquals(0.1f, functionScoreQueryBuilder.getMinScore(), 0.001f);
        assertEquals(2.0f, functionScoreQueryBuilder.boost(), 0.001f);

        // Verify functions
        assertNotNull(functionScoreQueryBuilder.filterFunctionBuilders());
        assertEquals(2, functionScoreQueryBuilder.filterFunctionBuilders().length);
    }

}
