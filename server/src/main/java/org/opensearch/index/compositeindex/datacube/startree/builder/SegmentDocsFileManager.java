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
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Class for managing segment documents file.
 * Segment documents are stored in a single file named 'segment.documents' for sorting and aggregation. A document ID array is created,
 * and the document IDs in the array are swapped during sorting based on the actual segment document values in the file.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SegmentDocsFileManager extends AbstractDocumentsFileManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(SegmentDocsFileManager.class);
    private static final String SEGMENT_DOC_FILE_NAME = "segment.documents";
    private IndexInput segmentDocsFileInput;
    private RandomAccessInput segmentRandomInput;
    final IndexOutput segmentDocsFileOutput;

    public SegmentDocsFileManager(SegmentWriteState state, StarTreeField starTreeField, List<MetricAggregatorInfo> metricAggregatorInfos)
        throws IOException {
        super(state, starTreeField, metricAggregatorInfos);
        try {
            segmentDocsFileOutput = tmpDirectory.createTempOutput(SEGMENT_DOC_FILE_NAME, state.segmentSuffix, state.context);
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(this);
            throw e;
        }
    }

    @Override
    public void writeStarTreeDocument(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) throws IOException {
        writeStarTreeDocument(starTreeDocument, segmentDocsFileOutput, isAggregatedDoc);
    }

    private void maybeInitializeSegmentInput() throws IOException {
        try {
            if (segmentDocsFileInput == null) {
                IOUtils.closeWhileHandlingException(segmentDocsFileOutput);
                segmentDocsFileInput = tmpDirectory.openInput(segmentDocsFileOutput.getName(), state.context);
                segmentRandomInput = segmentDocsFileInput.randomAccessSlice(0, segmentDocsFileInput.length());
            }
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(this);
            throw e;
        }
    }

    @Override
    public StarTreeDocument readStarTreeDocument(int docId, boolean isAggregatedDoc) throws IOException {
        maybeInitializeSegmentInput();
        return readStarTreeDocument(segmentRandomInput, (long) docId * docSizeInBytes, isAggregatedDoc);
    }

    @Override
    public Long[] readDimensions(int docId) throws IOException {
        maybeInitializeSegmentInput();
        Long[] dims = new Long[starTreeField.getDimensionsOrder().size()];
        readDimensions(dims, segmentRandomInput, (long) docId * docSizeInBytes);
        return dims;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        Long[] dims = readDimensions(docId);
        return dims[dimensionId];
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.segmentDocsFileOutput != null) {
                IOUtils.closeWhileHandlingException(segmentDocsFileOutput);
                tmpDirectory.deleteFile(segmentDocsFileOutput.getName());
            }
        } finally {
            IOUtils.closeWhileHandlingException(segmentDocsFileInput, segmentDocsFileOutput);
        }
    }
}
