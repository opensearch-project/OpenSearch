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
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.utils.CompensatedSumType;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeDocumentBitSetUtil;
import org.opensearch.index.mapper.FieldValueConverter;
import org.opensearch.search.aggregations.metrics.CompensatedSum;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG;

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
    protected final int numDimensions;

    public AbstractDocumentsFileManager(
        SegmentWriteState state,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        int numDimensions
    ) {
        this.starTreeField = starTreeField;
        this.tmpDirectory = new TrackingDirectoryWrapper(state.directory);
        this.metricAggregatorInfos = metricAggregatorInfos;
        this.state = state;
        numMetrics = metricAggregatorInfos.size();
        this.numDimensions = numDimensions;
    }

    private void setDocSizeInBytes(int numBytes) {
        if (docSizeInBytes == -1) {
            docSizeInBytes = numBytes;
        }
        assert docSizeInBytes == numBytes;
    }

    /**
     * Write the star tree document to a byte buffer
     */
    protected int writeStarTreeDocument(StarTreeDocument starTreeDocument, IndexOutput output, boolean isAggregatedDoc) throws IOException {
        int numBytes = calculateDocumentSize(starTreeDocument, isAggregatedDoc);
        byte[] bytes = new byte[numBytes];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        writeDimensions(starTreeDocument, buffer);
        if (isAggregatedDoc == false) {
            writeFlushMetrics(starTreeDocument, buffer);
        } else {
            writeMetrics(starTreeDocument, buffer, isAggregatedDoc);
        }
        output.writeBytes(bytes, bytes.length);
        setDocSizeInBytes(numBytes);
        return bytes.length;
    }

    /**
     * Write dimensions to the byte buffer
     */
    protected void writeDimensions(StarTreeDocument starTreeDocument, ByteBuffer buffer) throws IOException {
        for (Long dimension : starTreeDocument.dimensions) {
            buffer.putLong(dimension == null ? 0L : dimension);
        }
        StarTreeDocumentBitSetUtil.writeBitSet(starTreeDocument.dimensions, buffer);
    }

    /**
     * Write star tree document metrics to file
     */
    protected void writeFlushMetrics(StarTreeDocument starTreeDocument, ByteBuffer buffer) throws IOException {
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            buffer.putLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
        }
        StarTreeDocumentBitSetUtil.writeBitSet(starTreeDocument.metrics, buffer);
    }

    /**
     * Write star tree document metrics to the byte buffer
     */
    protected void writeMetrics(StarTreeDocument starTreeDocument, ByteBuffer buffer, boolean isAggregatedDoc) throws IOException {
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            FieldValueConverter aggregatedValueType = metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType();
            if (aggregatedValueType.equals(LONG)) {
                buffer.putLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
            } else if (aggregatedValueType.equals(DOUBLE)) {
                if (isAggregatedDoc) {
                    long val = NumericUtils.doubleToSortableLong(
                        starTreeDocument.metrics[i] == null ? 0.0 : (Double) starTreeDocument.metrics[i]
                    );
                    buffer.putLong(val);
                } else {
                    buffer.putLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
                }
            } else if (aggregatedValueType instanceof CompensatedSumType) {
                if (isAggregatedDoc) {
                    long val = NumericUtils.doubleToSortableLong(
                        starTreeDocument.metrics[i] == null ? 0.0 : ((CompensatedSum) starTreeDocument.metrics[i]).value()
                    );
                    buffer.putLong(val);
                } else {
                    buffer.putLong(starTreeDocument.metrics[i] == null ? 0L : (Long) starTreeDocument.metrics[i]);
                }
            } else {
                throw new IllegalStateException("Unsupported metric type");
            }
        }
        StarTreeDocumentBitSetUtil.writeBitSet(starTreeDocument.metrics, buffer);
    }

    /**
     * Calculate the size of the serialized StarTreeDocument
     */
    private int calculateDocumentSize(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) {
        int size = starTreeDocument.dimensions.length * Long.BYTES;
        size += getLength(starTreeDocument.dimensions);

        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            size += Long.BYTES;
        }
        size += getLength(starTreeDocument.metrics);

        return size;
    }

    private static int getLength(Object[] array) {
        return (array.length / 8) + (array.length % 8 == 0 ? 0 : 1);
    }

    /**
     * Reads the star tree document from file with given offset
     *
     * @param input   RandomAccessInput
     * @param offset  Offset in the file
     * @param isAggregatedDoc boolean to indicate if aggregated star tree docs should be read
     * @return StarTreeDocument
     * @throws IOException IOException in case of I/O errors
     */
    protected StarTreeDocument readStarTreeDocument(RandomAccessInput input, long offset, boolean isAggregatedDoc) throws IOException {
        Long[] dimensions = new Long[numDimensions];
        long initialOffset = offset;
        offset = readDimensions(dimensions, input, offset);

        Object[] metrics = new Object[numMetrics];
        if (isAggregatedDoc == false) {
            offset = readMetrics(input, offset, metrics);
        } else {
            offset = readMetrics(input, offset, numMetrics, metrics, isAggregatedDoc);
        }
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
     * Read metrics based on metric field values. Then we reuse the metric field values to each of the metric stats.
     */
    private long readMetrics(RandomAccessInput input, long offset, Object[] metrics) throws IOException {
        Object[] fieldMetrics = new Object[starTreeField.getMetrics().size()];
        for (int i = 0; i < starTreeField.getMetrics().size(); i++) {
            fieldMetrics[i] = input.readLong(offset);
            offset += Long.BYTES;
        }
        offset += StarTreeDocumentBitSetUtil.readBitSet(input, offset, fieldMetrics, index -> null);
        int fieldIndex = 0;
        int numMetrics = 0;
        for (Metric metric : starTreeField.getMetrics()) {
            for (MetricStat stat : metric.getBaseMetrics()) {
                metrics[numMetrics] = fieldMetrics[fieldIndex];
                numMetrics++;
            }
            fieldIndex++;
        }
        return offset;
    }

    /**
     * Read star tree metrics from file
     */
    private long readMetrics(RandomAccessInput input, long offset, int numMetrics, Object[] metrics, boolean isAggregatedDoc)
        throws IOException {
        for (int i = 0; i < numMetrics; i++) {
            FieldValueConverter aggregatedValueType = metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType();
            if (aggregatedValueType.equals(LONG)) {
                metrics[i] = input.readLong(offset);
                offset += Long.BYTES;
            } else if (aggregatedValueType.equals(DOUBLE)) {
                long val = input.readLong(offset);
                if (isAggregatedDoc) {
                    metrics[i] = DOUBLE.toDoubleValue(val);
                } else {
                    metrics[i] = val;
                }
                offset += Long.BYTES;
            } else if (aggregatedValueType instanceof CompensatedSumType) {
                long val = input.readLong(offset);
                if (isAggregatedDoc) {
                    metrics[i] = new CompensatedSum(aggregatedValueType.toDoubleValue(val), 0);
                } else {
                    metrics[i] = val;
                }
                offset += Long.BYTES;
            } else {
                throw new IllegalStateException("Unsupported metric type");
            }
        }
        offset += StarTreeDocumentBitSetUtil.readBitSet(input, offset, metrics, index -> null);
        return offset;
    }

    /**
     * Write star tree document to file
     */
    public abstract void writeStarTreeDocument(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) throws IOException;

    /**
     * Read star tree document from file based on doc id
     */
    public abstract StarTreeDocument readStarTreeDocument(int docId, boolean isAggregatedDoc) throws IOException;

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
