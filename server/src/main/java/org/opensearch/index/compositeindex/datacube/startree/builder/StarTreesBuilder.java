/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builder to construct star-trees based on multiple star-tree fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);

    private final List<StarTreeField> starTreeFields;
    private final SegmentWriteState state;
    private final MapperService mapperService;

    public StarTreesBuilder(SegmentWriteState segmentWriteState, MapperService mapperService) {
        List<StarTreeField> starTreeFields = new ArrayList<>();
        for (CompositeMappedFieldType compositeMappedFieldType : mapperService.getCompositeFieldTypes()) {
            if (compositeMappedFieldType instanceof StarTreeMapper.StarTreeFieldType) {
                StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) compositeMappedFieldType;
                starTreeFields.add(
                    new StarTreeField(
                        starTreeFieldType.name(),
                        starTreeFieldType.getDimensions(),
                        starTreeFieldType.getMetrics(),
                        starTreeFieldType.getStarTreeConfig()
                    )
                );
            }
        }
        this.starTreeFields = starTreeFields;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
    }

    /**
     * Builds the star-trees.
     */
    public void build(Map<String, DocValuesProducer> fieldProducerMap) throws IOException {
        if (starTreeFields.isEmpty()) {
            logger.debug("no star-tree fields found, returning from star-tree builder");
            return;
        }
        long startTime = System.currentTimeMillis();

        int numStarTrees = starTreeFields.size();
        logger.debug("Starting building {} star-trees with star-tree fields", numStarTrees);

        // Build all star-trees
        for (StarTreeField starTreeField : starTreeFields) {
            try (StarTreeBuilder starTreeBuilder = getSingleTreeBuilder(starTreeField, state, mapperService)) {
                starTreeBuilder.build(fieldProducerMap);
            }
        }
        logger.debug("Took {} ms to build {} star-trees with star-tree fields", System.currentTimeMillis() - startTime, numStarTrees);
    }

    @Override
    public void close() throws IOException {
        // TODO : close files
    }

    /**
     * Merges star tree fields from multiple segments
     *
     * @param starTreeValuesSubsPerField   starTreeValuesSubs per field
     */
    public void buildDuringMerge(final Map<String, List<StarTreeValues>> starTreeValuesSubsPerField) throws IOException {
        logger.debug("Starting merge of {} star-trees with star-tree fields", starTreeValuesSubsPerField.size());
        long startTime = System.currentTimeMillis();
        for (Map.Entry<String, List<StarTreeValues>> entry : starTreeValuesSubsPerField.entrySet()) {
            List<StarTreeValues> starTreeValuesList = entry.getValue();
            if (starTreeValuesList.isEmpty()) {
                logger.debug("StarTreeValues is empty for all segments for field : {}", entry.getKey());
                continue;
            }
            StarTreeField starTreeField = starTreeValuesList.get(0).getStarTreeField();
            StarTreeBuilder builder = getSingleTreeBuilder(starTreeField, state, mapperService);
            builder.build(starTreeValuesList);
            builder.close();
        }
        logger.debug(
            "Took {} ms to merge {} star-trees with star-tree fields",
            System.currentTimeMillis() - startTime,
            starTreeValuesSubsPerField.size()
        );
    }

    /**
     * Get star-tree builder based on build mode.
     */
    StarTreeBuilder getSingleTreeBuilder(StarTreeField starTreeField, SegmentWriteState state, MapperService mapperService)
        throws IOException {
        switch (starTreeField.getStarTreeConfig().getBuildMode()) {
            case ON_HEAP:
                return new OnHeapStarTreeBuilder(starTreeField, state, mapperService);
            case OFF_HEAP:
                // TODO
                // return new OffHeapStarTreeBuilder(starTreeField, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No star tree implementation is available for [%s] build mode",
                        starTreeField.getStarTreeConfig().getBuildMode()
                    )
                );
        }
    }
}
