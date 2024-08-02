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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericTypeConverters;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeDocumentBitSetUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Abstract class for managing star tree file operations.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class AbstractDocumentsFileManager implements Closeable {
    private static final Logger logger = LogManager.getLogger(AbstractDocumentsFileManager.class);
    protected final StarTreeField starTreeField;
    protected final List<MetricAggregatorInfo> metricAggregatorInfos;
    protected final int numMetrics;
    protected final TrackingDirectoryWrapper tmpDirectory;
    protected final SegmentWriteState state;
    protected int docSizeInBytes = -1;

    public AbstractDocumentsFileManager(
        SegmentWriteState state,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos
    ) {
        this.starTreeField = starTreeField;
        this.tmpDirectory = new TrackingDirectoryWrapper(state.directory);
        this.metricAggregatorInfos = metricAggregatorInfos;
        this.state = state;
        numMetrics = metricAggregatorInfos.size();
    }

    private void setDocSizeInBytes(int numBytes) {
        if (docSizeInBytes == -1) {
            docSizeInBytes = numBytes;
        }
        assert docSizeInBytes == numBytes;
    }

    /**
     * Write the star tree document to file associated with dimensions and metrics
     */
    protected int writeStarTreeDocument(StarTreeDocument starTreeDocument, IndexOutput output, boolean isAggregatedDoc) throws IOException {
        int numBytes = writeDimensions(starTreeDocument, output);
        numBytes += writeMetrics(starTreeDocument, output, isAggregatedDoc);
        setDocSizeInBytes(numBytes);
        return numBytes;
    }

    /**
     * Write dimensions to file
     */
    protected int writeDimensions(StarTreeDocument starTreeDocument, IndexOutput output) throws IOException {
        int numBytes = 0;
        for (int i = 0; i < starTreeDocument.dimensions.length; i++) {
            output.writeLong(starTreeDocument.dimensions[i] == null ? 0L : starTreeDocument.dimensions[i]);
            numBytes += Long.BYTES;
        }
        numBytes += StarTreeDocumentBitSetUtil.writeBitSet(starTreeDocument.dimensions, output);
        return numBytes;
    }

    /**
     * Write star tree document metrics to file
     */
    protected int writeMetrics(StarTreeDocument starTreeDocument, IndexOutput output, boolean isAggregatedDoc) throws IOException {
        int numBytes = 0;
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    output.writeLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
                    numBytes += Long.BYTES;
                    break;
                case DOUBLE:
                    if (isAggregatedDoc) {
                        long val = NumericUtils.doubleToSortableLong(
                            starTreeDocument.metrics[i] == null ? 0.0 : (Double) starTreeDocument.metrics[i]
                        );
                        output.writeLong(val);
                    } else {
                        output.writeLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
                    }
                    numBytes += Long.BYTES;
                    break;
                default:
                    throw new IllegalStateException("Unsupported metric type");
            }
        }
        numBytes += StarTreeDocumentBitSetUtil.writeBitSet(starTreeDocument.metrics, output);
        return numBytes;
    }

    /**
     * Reads the star tree document from file with given offset
     *
     * @param input   RandomAccessInput
     * @param offset  Offset in the file
     * @param shouldReadAggregatedDocs boolean to indicate if aggregated star tree docs should be read
     * @return StarTreeDocument
     * @throws IOException IOException in case of I/O errors
     */
    protected StarTreeDocument readStarTreeDocument(RandomAccessInput input, long offset, boolean shouldReadAggregatedDocs)
        throws IOException {
        int dimSize = starTreeField.getDimensionsOrder().size();
        Long[] dimensions = new Long[dimSize];
        long initialOffset = offset;
        offset = readDimensions(dimensions, input, offset);

        Object[] metrics = new Object[numMetrics];
        offset = readMetrics(input, offset, numMetrics, metrics, shouldReadAggregatedDocs);
        assert (offset - initialOffset) == docSizeInBytes;
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Read dimensions from file
     */
    protected long readDimensions(Long[] dimensions, RandomAccessInput input, long offset) throws IOException {
        for (int i = 0; i < dimensions.length; i++) {
            try {
                dimensions[i] = input.readLong(offset);
            } catch (Exception e) {
                logger.error("Error reading dimension value at offset {} for dimension {}", offset, i);
                throw e;
            }
            offset += Long.BYTES;
        }
        offset += StarTreeDocumentBitSetUtil.readBitSet(input, offset, dimensions, index -> null);
        return offset;
    }

    /**
     * Read star tree metrics from file
     */
    protected long readMetrics(RandomAccessInput input, long offset, int numMetrics, Object[] metrics, boolean shouldReadAggregatedDocs)
        throws IOException {
        for (int i = 0; i < numMetrics; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    metrics[i] = input.readLong(offset);
                    offset += Long.BYTES;
                    break;
                case DOUBLE:
                    long val = input.readLong(offset);
                    if (shouldReadAggregatedDocs) {
                        metrics[i] = StarTreeNumericTypeConverters.sortableLongtoDouble(val);
                    } else {
                        metrics[i] = val;
                    }
                    offset += Long.BYTES;
                    break;
                default:
                    throw new IllegalStateException("Unsupported metric type");
            }
        }
        offset += StarTreeDocumentBitSetUtil.readBitSet(
            input,
            offset,
            metrics,
            index -> metricAggregatorInfos.get(index).getValueAggregators().getIdentityMetricValue()
        );
        return offset;
    }

    /**
     * Write star tree document to file
     */
    public abstract void writeStarTreeDocument(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) throws IOException;

    /**
     * Read star tree document from file based on doc id
     */
    public abstract StarTreeDocument readStarTreeDocument(int docId, boolean isMerge) throws IOException;

    /**
     * Read star document dimensions from file based on doc id
     */
    public abstract Long[] readDimensions(int docId) throws IOException;

    /**
     * Read dimension value for given doc id and dimension id
     */
    public abstract Long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Delete the temporary files created
     */
    public void deleteFiles(boolean success) throws IOException {
        if (success) {
            for (String file : tmpDirectory.getCreatedFiles()) {
                tmpDirectory.deleteFile(file);
            }
        } else {
            deleteFilesIgnoringException();
        }

    }

    /**
     * Delete the temporary files created
     */
    private void deleteFilesIgnoringException() throws IOException {
        for (String file : tmpDirectory.getCreatedFiles()) {
            try {
                tmpDirectory.deleteFile(file);
            } catch (final IOException ignored) {} // similar to IOUtils.deleteFilesWhileIgnoringExceptions
        }
    }
}
