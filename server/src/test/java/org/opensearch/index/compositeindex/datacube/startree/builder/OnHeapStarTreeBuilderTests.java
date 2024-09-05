/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

public class OnHeapStarTreeBuilderTests extends AbstractStarTreeBuilderTests {

    @Override
    public BaseStarTreeBuilder getStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        return new OnHeapStarTreeBuilder(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
    }

    @Override
    StarTreeFieldConfiguration.StarTreeBuildMode getBuildMode() {
        return StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP;
    }

}
