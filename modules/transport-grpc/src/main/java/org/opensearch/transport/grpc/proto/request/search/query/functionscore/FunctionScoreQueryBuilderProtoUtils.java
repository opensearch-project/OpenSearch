/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.WeightBuilder;
import org.opensearch.protobufs.FunctionBoostMode;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.FunctionScoreMode;
import org.opensearch.protobufs.FunctionScoreQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting FunctionScoreQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of function score queries
 * into their corresponding OpenSearch FunctionScoreQueryBuilder implementations for search operations.
 */
class FunctionScoreQueryBuilderProtoUtils {

    private FunctionScoreQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FunctionScoreQuery to an OpenSearch FunctionScoreQueryBuilder.
     * Similar to {@link FunctionScoreQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * FunctionScoreQueryBuilder with the appropriate query, functions, boost mode, score mode,
     * max boost, min score, boost, and query name settings.
     *
     * @param functionScoreQueryProto The Protocol Buffer FunctionScoreQuery object
     * @param registry The registry to use for converting nested queries
     * @return A configured FunctionScoreQueryBuilder instance
     * @throws IllegalArgumentException if the function score query is invalid or contains unsupported function types
     */
    static FunctionScoreQueryBuilder fromProto(FunctionScoreQuery functionScoreQueryProto, QueryBuilderProtoConverterRegistry registry) {
        if (functionScoreQueryProto == null) {
            throw new IllegalArgumentException("FunctionScoreQuery cannot be null");
        }

        QueryBuilder query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode scoreMode = FunctionScoreQueryBuilder.DEFAULT_SCORE_MODE;
        float maxBoost = org.opensearch.common.lucene.search.function.FunctionScoreQuery.DEFAULT_MAX_BOOST;
        Float minScore = null;

        CombineFunction combineFunction = null;
        List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>();

        if (functionScoreQueryProto.hasQuery()) {
            QueryContainer queryContainer = functionScoreQueryProto.getQuery();
            query = registry.fromProto(queryContainer);
        }

        if (functionScoreQueryProto.getFunctionsCount() > 0) {
            for (FunctionScoreContainer container : functionScoreQueryProto.getFunctionsList()) {
                FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunctionBuilder = parseFunctionScoreContainer(container, registry);
                filterFunctionBuilders.add(filterFunctionBuilder);
            }
        }

        if (query == null) {
            query = new MatchAllQueryBuilder();
        }

        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(
            query,
            filterFunctionBuilders.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0])
        );

        if (functionScoreQueryProto.hasBoostMode()
            && functionScoreQueryProto.getBoostMode() != FunctionBoostMode.FUNCTION_BOOST_MODE_UNSPECIFIED) {
            combineFunction = parseBoostMode(functionScoreQueryProto.getBoostMode());
        }

        if (functionScoreQueryProto.hasScoreMode()) {
            scoreMode = parseScoreMode(functionScoreQueryProto.getScoreMode());
        }

        if (functionScoreQueryProto.hasMaxBoost()) {
            maxBoost = functionScoreQueryProto.getMaxBoost();
        }

        if (functionScoreQueryProto.hasMinScore()) {
            minScore = functionScoreQueryProto.getMinScore();
        }

        if (functionScoreQueryProto.hasBoost()) {
            boost = functionScoreQueryProto.getBoost();
        }

        if (functionScoreQueryProto.hasXName()) {
            queryName = functionScoreQueryProto.getXName();
        }

        functionScoreQueryBuilder.boost(boost);
        if (queryName != null) {
            functionScoreQueryBuilder.queryName(queryName);
        }
        functionScoreQueryBuilder.scoreMode(scoreMode);
        functionScoreQueryBuilder.maxBoost(maxBoost);
        if (minScore != null) {
            functionScoreQueryBuilder.setMinScore(minScore);
        }
        if (combineFunction != null) {
            functionScoreQueryBuilder.boostMode(combineFunction);
        }

        return functionScoreQueryBuilder;
    }

    /**
     * Parses a FunctionScoreContainer and creates the appropriate FilterFunctionBuilder.
     */
    private static FunctionScoreQueryBuilder.FilterFunctionBuilder parseFunctionScoreContainer(
        FunctionScoreContainer container,
        QueryBuilderProtoConverterRegistry registry
    ) {
        if (container == null) {
            throw new IllegalArgumentException("FunctionScoreContainer cannot be null");
        }

        QueryBuilder filter = null;
        ScoreFunctionBuilder<?> scoreFunction = null;
        Float functionWeight = null;
        if (container.hasFilter()) {
            QueryContainer filterContainer = container.getFilter();
            filter = registry.fromProto(filterContainer);
        }

        // Check for weight (only set if present, otherwise use default)
        if (container.hasWeight()) {
            functionWeight = container.getWeight();
        }

        // Parse the function score container directly
        // If there's no function type but there's a weight, create a WeightBuilder
        if (container.getFunctionScoreContainerCase() == FunctionScoreContainer.FunctionScoreContainerCase.FUNCTIONSCORECONTAINER_NOT_SET) {
            scoreFunction = null;
        } else {
            scoreFunction = parseScoreFunction(container);
        }

        if (functionWeight != null) {
            if (scoreFunction == null) {
                scoreFunction = new WeightBuilder().setWeight(functionWeight);
            } else {
                scoreFunction.setWeight(functionWeight);
            }
        }

        if (filter == null) {
            filter = new MatchAllQueryBuilder();
        }

        return new FunctionScoreQueryBuilder.FilterFunctionBuilder(filter, scoreFunction);
    }

    /**
     * Parses a FunctionScoreContainer and creates the appropriate ScoreFunctionBuilder.
     */
    private static ScoreFunctionBuilder<?> parseScoreFunction(FunctionScoreContainer container) {
        if (container == null) {
            throw new IllegalArgumentException("FunctionScoreContainer cannot be null");
        }

        FunctionScoreContainer.FunctionScoreContainerCase functionCase = container.getFunctionScoreContainerCase();

        return switch (functionCase) {
            case FIELD_VALUE_FACTOR -> new FieldValueFactorFunctionProtoConverter().fromProto(container.getFieldValueFactor());
            case RANDOM_SCORE -> new RandomScoreFunctionProtoConverter().fromProto(container.getRandomScore());
            case SCRIPT_SCORE -> new ScriptScoreFunctionProtoConverter().fromProto(container.getScriptScore());
            case EXP -> new ExpDecayFunctionProtoConverter().fromProto(container.getExp());
            case GAUSS -> new GaussDecayFunctionProtoConverter().fromProto(container.getGauss());
            case LINEAR -> new LinearDecayFunctionProtoConverter().fromProto(container.getLinear());
            default -> throw new IllegalArgumentException("Unsupported function score type: " + functionCase);
        };
    }

    /**
     * Parses a FunctionBoostMode enum to CombineFunction.
     */
    private static CombineFunction parseBoostMode(FunctionBoostMode boostMode) {
        switch (boostMode) {
            case FUNCTION_BOOST_MODE_AVG:
                return CombineFunction.AVG;
            case FUNCTION_BOOST_MODE_MAX:
                return CombineFunction.MAX;
            case FUNCTION_BOOST_MODE_MIN:
                return CombineFunction.MIN;
            case FUNCTION_BOOST_MODE_MULTIPLY:
                return CombineFunction.MULTIPLY;
            case FUNCTION_BOOST_MODE_REPLACE:
                return CombineFunction.REPLACE;
            case FUNCTION_BOOST_MODE_SUM:
                return CombineFunction.SUM;
            default:
                throw new IllegalArgumentException("Unsupported boost mode: " + boostMode);
        }
    }

    /**
     * Parses a FunctionScoreMode enum to FunctionScoreQuery.ScoreMode.
     */
    private static org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode parseScoreMode(FunctionScoreMode scoreMode) {
        switch (scoreMode) {
            case FUNCTION_SCORE_MODE_AVG:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.AVG;
            case FUNCTION_SCORE_MODE_FIRST:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.FIRST;
            case FUNCTION_SCORE_MODE_MAX:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MAX;
            case FUNCTION_SCORE_MODE_MIN:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MIN;
            case FUNCTION_SCORE_MODE_MULTIPLY:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.MULTIPLY;
            case FUNCTION_SCORE_MODE_SUM:
                return org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode.SUM;
            default:
                return FunctionScoreQueryBuilder.DEFAULT_SCORE_MODE;
        }
    }

}
