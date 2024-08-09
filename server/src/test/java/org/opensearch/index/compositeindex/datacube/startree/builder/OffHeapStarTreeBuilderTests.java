/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

public class OffHeapStarTreeBuilderTests extends AbstractStarTreeBuilderTests {
    @Override
    public BaseStarTreeBuilder getStarTreeBuilder(
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        return new OffHeapStarTreeBuilder(starTreeField, segmentWriteState, mapperService);
    }
}
