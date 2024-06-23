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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);

    private final List<StarTreeField> starTreeFields;
    private final StarTreeFieldConfiguration.StarTreeBuildMode buildMode;
    private final DocValuesConsumer docValuesConsumer;
    private final SegmentWriteState state;
    private final DocValuesProducer docValuesProducer;
    private final MapperService mapperService;

    public StarTreesBuilder(
        List<StarTreeField> starTreeFields,
        StarTreeFieldConfiguration.StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) {
        this.starTreeFields = starTreeFields;
        if (starTreeFields == null || starTreeFields.isEmpty()) {
            throw new IllegalArgumentException("Must provide star-tree builder configs");
        }
        this.buildMode = buildMode;
        this.docValuesProducer = docValuesProducer;
        this.docValuesConsumer = docValuesConsumer;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
    }

    /**
     * Builds the star-trees.
     */
    public void build() throws Exception {
        long startTime = System.currentTimeMillis();
        int numStarTrees = starTreeFields.size();
        logger.debug("Starting building {} star-trees with configs: {} using {} builder", numStarTrees, starTreeFields, buildMode);

        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            StarTreeField starTreeField = starTreeFields.get(i);
            try (
                SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(
                    starTreeField,
                    buildMode,
                    docValuesProducer,
                    docValuesConsumer,
                    state,
                    mapperService
                )
            ) {
                singleTreeBuilder.build();
            }
        }
        logger.debug(
            "Took {} ms to building {} star-trees with configs: {} using {} builder",
            System.currentTimeMillis() - startTime,
            numStarTrees,
            starTreeFields,
            buildMode
        );
    }

    @Override
    public void close() throws IOException {

    }

    private static SingleTreeBuilder getSingleTreeBuilder(
        StarTreeField starTreeField,
        StarTreeFieldConfiguration.StarTreeBuildMode buildMode,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (buildMode) {
            case ON_HEAP:
                return new OnHeapSingleTreeBuilder(starTreeField, docValuesProducer, docValuesConsumer, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "No star tree implementation is available for [%s] build mode", buildMode)
                );
        }
    }
}
