/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Factory for query results normalization processor for search pipeline. Instantiates processor based on user provided input.
 */
public class NormalizationProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    private static final Logger logger = LogManager.getLogger(NormalizationProcessorFactory.class);

    public static final String NORMALIZATION_CLAUSE = "normalization";
    public static final String COMBINATION_CLAUSE = "combination";
    public static final String TECHNIQUE = "technique";
    public static final String PARAMETERS = "parameters";

    private final NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private ScoreNormalizationFactory scoreNormalizationFactory;
    private ScoreCombinationFactory scoreCombinationFactory;

    public NormalizationProcessorFactory(
        NormalizationProcessorWorkflow normalizationProcessorWorkflow,
        ScoreNormalizationFactory scoreNormalizationFactory,
        ScoreCombinationFactory scoreCombinationFactory
    ) {
        this.normalizationProcessorWorkflow = normalizationProcessorWorkflow;
        this.scoreNormalizationFactory = scoreNormalizationFactory;
        this.scoreCombinationFactory = scoreCombinationFactory;
    }

    @Override
    public SearchPhaseResultsProcessor create(
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) throws Exception {
        Map<String, Object> normalizationClause = readOptionalMap(NormalizationProcessor.TYPE, tag, config, NORMALIZATION_CLAUSE);
        ScoreNormalizationTechnique normalizationTechnique = ScoreNormalizationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(normalizationClause)) {
            String normalizationTechniqueName = readStringProperty(
                NormalizationProcessor.TYPE,
                tag,
                normalizationClause,
                TECHNIQUE,
                MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME
            );
            Map<String, Object> normalizationParams = readOptionalMap(NormalizationProcessor.TYPE, tag, normalizationClause, PARAMETERS);
            normalizationTechnique = scoreNormalizationFactory.createNormalization(normalizationTechniqueName, normalizationParams);
        }

        Map<String, Object> combinationClause = readOptionalMap(NormalizationProcessor.TYPE, tag, config, COMBINATION_CLAUSE);

        ScoreCombinationTechnique scoreCombinationTechnique = ScoreCombinationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = readStringProperty(
                NormalizationProcessor.TYPE,
                tag,
                combinationClause,
                TECHNIQUE,
                ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME
            );
            // check for optional combination params
            Map<String, Object> combinationParams = readOptionalMap(NormalizationProcessor.TYPE, tag, combinationClause, PARAMETERS);
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique, combinationParams);
        }

        TechniqueCompatibilityCheckDTO techniqueCompatibilityCheckDTO = TechniqueCompatibilityCheckDTO.builder()
            .scoreNormalizationTechnique(normalizationTechnique)
            .scoreCombinationTechnique(scoreCombinationTechnique)
            .build();
        scoreNormalizationFactory.isTechniquesCompatible(techniqueCompatibilityCheckDTO);

        logger.info(
            "Creating search phase results processor of type [{}] with normalization [{}] and combination [{}]",
            NormalizationProcessor.TYPE,
            normalizationTechnique,
            scoreCombinationTechnique
        );
        return new NormalizationProcessor(
            tag,
            description,
            normalizationTechnique,
            scoreCombinationTechnique,
            normalizationProcessorWorkflow
        );
    }
}
