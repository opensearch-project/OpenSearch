/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.nio.file.Path;

public class OffHeapStarTreeBuilderTests extends AbstractStarTreeBuilderTests {
    @Override
    public BaseStarTreeBuilder getStarTreeBuilder(
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        return new OffHeapStarTreeBuilder(starTreeField, segmentWriteState, mapperService, randomIntBetween(2, 6));
    }

    public void testDimensions() throws IOException {
        for (int k = 0; k < 10; k++) {
            int randomDimensionSize = randomIntBetween(2, 63);
            Long[] dims = new Long[randomDimensionSize];
            for (int i = 0; i < randomDimensionSize; i++) {
                dims[i] = randomLong();
            }
            assertNullAndValuesInDims(dims);
        }
    }

    private void assertNullAndValuesInDims(Long[] dims) throws IOException {
        int randomNullIndex1 = randomIntBetween(0, dims.length - 1);
        int randomNullIndex2 = randomIntBetween(0, dims.length - 1);
        dims[randomNullIndex1] = null;
        dims[randomNullIndex2] = null;
        Object[] metrics = new Object[64];
        StarTreeDocument doc = new StarTreeDocument(dims, metrics);

        Path basePath = createTempDir("OffHeapTests");
        FSDirectory fsDirectory = FSDirectory.open(basePath);
        String TEST_FILE = "test_file";
        IndexOutput indexOutput = fsDirectory.createOutput(TEST_FILE, IOContext.DEFAULT);
        OffHeapStarTreeBuilder builder = (OffHeapStarTreeBuilder) getStarTreeBuilder(compositeField, writeState, mapperService);
        builder.writeDimensions(doc, indexOutput);
        indexOutput.close();
        Long[] dims1 = new Long[dims.length];
        IndexInput in = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);
        RandomAccessInput randomAccessInput = in.randomAccessSlice(0, in.length());
        builder.readDimensions(dims1, randomAccessInput, 0);
        for (int i = 0; i < dims.length; i++) {
            if (i == randomNullIndex1 || i == randomNullIndex2) {
                assertNull(dims1[i]);
            } else {
                assertEquals(dims[i], dims1[i]);
            }
        }
        in.close();
        builder.close();
        fsDirectory.close();
    }
}
