/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.vector.complex.BaseLargeRepeatedValueViewVector.OFFSET_WIDTH;
import static org.apache.arrow.vector.complex.BaseLargeRepeatedValueViewVector.SIZE_WIDTH;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

/** A consumer for reading messages from an ingestion shard in arrow format */
public class ArrowShardConsumer implements IngestionShardConsumer<ArrowOffset, ArrowMessage> {

    // fixed field name for index operation message
    private static final String OP_TYPE = "_op_type";
    private static final String SOURCE = "_source";
    private static final String VERSION = "_version";
    private static final String ID = "_id";

    /**
     * Use "index" rather than "create" so a replayed row during failure recovery overwrites the
     * existing document instead of leaving a duplicate Lucene document behind.
     */
    private static final String OP_INDEX = "index";

    private static final Logger LOGGER = LogManager.getLogger(ArrowShardConsumer.class);

    private final int shardId;
    private final ArrowSource arrowSource;
    private final ArrowIngestionConfig ingestionConfig;
    // Exclusive upper bound on the global row number this consumer will scan: the total rows in
    // the underlying dataset, or ArrowIngestionConfig#getRowBound() when configured and smaller.
    // Captured at construction time (the dataset is pinned to a single version, so its row count
    // does not change). Used as the upper bound when deciding whether this consumer has more rows
    // left to scan.
    private final long rowBound;

    // nextRead is only ever read/written by the single thread driving readNext, so a plain field
    // is sufficient.
    private long nextRead; // default to 0

    /**
     * Ctor
     *
     * @param shardId shard id
     * @param arrowSource underlying arrow source
     * @param ingestionConfig common ingestion configuration shared by all shards of the index
     */
    public ArrowShardConsumer(
            int shardId, ArrowSource arrowSource, ArrowIngestionConfig ingestionConfig) {
        this.shardId = shardId;
        this.arrowSource = arrowSource;
        this.ingestionConfig = ingestionConfig;
        long datasetRowCount = arrowSource.getRowCount();
        Long configuredRowBound = ingestionConfig.getRowBound();
        this.rowBound =
                configuredRowBound == null
                        ? datasetRowCount
                        : Math.min(configuredRowBound, datasetRowCount);
    }

    @Override
    public List<ReadResult<ArrowOffset, ArrowMessage>> readNext(
            ArrowOffset offset, boolean includeStart, long maxMessages, int timeoutMillis)
            throws TimeoutException {
        nextRead = includeStart ? offset.getOffset() : offset.getOffset() + 1;
        LOGGER.info("Resetting nextRead to: {}", nextRead);
        return readNext(maxMessages, timeoutMillis);
    }

    @Override
    public List<ReadResult<ArrowOffset, ArrowMessage>> readNext(long maxMessages, int timeoutMillis)
            throws TimeoutException {
        List<ReadResult<ArrowOffset, ArrowMessage>> readResults = new ArrayList<>();
        long initialRow = nextRead;
        if (nextRead >= rowBound) {
            LOGGER.debug(
                    "Scanned and returned 0 messages as nextRead: {} has reached the row bound: {}",
                    nextRead,
                    rowBound);
            return readResults;
        }
        long chunkEnd = Math.min(rowBound, nextRead + maxMessages);
        // Bound the scan's own declared limit to exactly maxMessages (rather than the whole
        // remaining dataset) and always read it to natural exhaustion instead of breaking out
        // early once enough shard-matches are found: some arrow source implementations (e.g. one
        // backed by Lance's Rust scanner) leak native memory when a scan is closed before it
        // reaches its declared limit, no matter how small that limit is. Sharding filters rows
        // client-side, so a call may scan maxMessages rows and return fewer than maxMessages
        // matches -- that's fine, the caller polls again to make further progress, same as e.g. a
        // Kafka consumer's poll() returning fewer records than max.poll.records.
        try (ArrowReader reader = arrowSource.getReader(nextRead, chunkEnd)) {
            Long versionTimestamp = arrowSource.getVersionTimestamp();
            String version = arrowSource.getVersion();
            while (reader.loadNextBatch()) {
                VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
                for (int i = 0; i < vsr.getRowCount(); i++) {
                    if (belongToThisShard(vsr, i)) {
                        readResults.add(
                                new ReadResult<>(
                                        new ArrowOffset(nextRead),
                                        parseRow(vsr, i, version, versionTimestamp, nextRead)));
                    }
                    nextRead++;
                }
            }
            LOGGER.debug(
                    "Scanned {} and returned {} messages",
                    nextRead - initialRow,
                    readResults.size());
            return readResults;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe); // there could be multiple places have IOE thrown
        }
    }

    /**
     * parse the row at the provided index to an arrow message
     *
     * @param vsr VectorSchemaRoot for the current batch of read
     * @param index local row number
     * @param version version in string
     * @param versionTimestamp timestamp for current version, see {@link
     *     ArrowSource#getVersionTimestamp()}
     * @param globalRow global row number
     */
    private ArrowMessage parseRow(
            VectorSchemaRoot vsr, int index, String version, Long versionTimestamp, long globalRow)
            throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        addIdColumn(vsr, index, globalRow, builder);
        builder.field(VERSION, version);
        builder.field(OP_TYPE, OP_INDEX);
        builder.field(SOURCE).startObject();
        for (FieldVector fv : vsr.getFieldVectors()) {
            if (fv.isNull(index)) {
                // Skip field if the value is null
                continue;
            }
            builder.field(fv.getField().getName());
            parseOneColumn(fv, index, builder);
        }
        builder.endObject(); // source object
        builder.endObject(); // whole message object
        return new ArrowMessage(builder, versionTimestamp);
    }

    private void addIdColumn(
            VectorSchemaRoot vsr, int index, long globalRow, XContentBuilder builder)
            throws IOException {
        if (ingestionConfig.getIdColumn() == null || ingestionConfig.getIdColumn().isEmpty()) {
            // no id column configured, use global row number as unique ID
            builder.field(ID, "" + globalRow);
        } else {
            builder.field(ID);
            FieldVector fieldVector = vsr.getVector(ingestionConfig.getIdColumn());
            if (fieldVector == null) {
                throw new IllegalArgumentException(
                        "Configured id column: "
                                + ingestionConfig.getIdColumn()
                                + " cannot be found from VectorSchemaRoot");
            }
            parseIdColumn(fieldVector, index, globalRow, builder);
        }
    }

    /**
     * builder should have already initiated the field (already called builder.field("xx")) before
     * calling this method
     */
    private void parseOneColumn(ValueVector vector, int row, XContentBuilder builder)
            throws IOException {
        if (vector.isNull(row)) {
            builder.nullValue();
            return;
        }
        if (vector.getField().getType().isComplex()) {
            parseOneComplexColumn(vector, row, builder);
        } else {
            parseOneSimpleColumn(vector, row, builder);
        }
    }

    private void parseIdColumn(
            ValueVector vector, int localRow, long globalRow, XContentBuilder builder)
            throws IOException {
        if (vector.isNull(localRow)) {
            throw new IllegalArgumentException(
                    "Document has null value set in the configured id column '"
                            + ingestionConfig.getIdColumn()
                            + "'. global row number: "
                            + globalRow);
        }
        try {
            parseStrTypeColumn(vector, localRow, builder, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Error parsing id column: "
                            + vector.getField().getName()
                            + " for doc with global row number: "
                            + globalRow,
                    e);
        }
    }

    /** Separate the string column handling out for generating _id field */
    private void parseStrTypeColumn(
            ValueVector vector, int row, XContentBuilder builder, boolean throwOnEmpty)
            throws IOException {
        String value =
                switch (vector.getField().getType().getTypeID()) {
                    case Utf8 ->
                            new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
                    case Utf8View ->
                            new String(
                                    ((ViewVarCharVector) vector).get(row), StandardCharsets.UTF_8);
                    case LargeUtf8 ->
                            new String(
                                    ((LargeVarCharVector) vector).get(row), StandardCharsets.UTF_8);
                    default -> {
                        // throwing IAE here because if the method is entered from
                        // parseOneSimpleColumn the type will be already checked,
                        // so the only situation where IAE will be thrown is when
                        // entered from parseIdColumn
                        throw new IllegalArgumentException(
                                "Field '"
                                        + vector.getField().getName()
                                        + "' has type : "
                                        + vector.getField().getType().getTypeID()
                                        + " which is not one of the expected string types.");
                    }
                };
        if (throwOnEmpty && value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Field '" + vector.getField().getName() + "' is empty");
            // here we only have local row number so it doesn't make
            // sense to attach the row here
        } else {
            builder.value(value);
        }
    }

    /**
     * Parsing content of a primitive column, mainly referring to {@link Types} for concrete field
     * vector types
     */
    private void parseOneSimpleColumn(ValueVector vector, int row, XContentBuilder builder)
            throws IOException {
        assert !vector.getField().getType().isComplex();
        switch (vector.getField().getType().getTypeID()) {
            case Null -> {
                builder.nullValue();
            }
            case Int -> {
                switch (vector.getMinorType()) {
                    case TINYINT -> builder.value(((TinyIntVector) vector).get(row));
                    case SMALLINT -> builder.value(((SmallIntVector) vector).get(row));
                    case INT -> builder.value(((IntVector) vector).get(row));
                    case BIGINT -> builder.value(((BigIntVector) vector).get(row));
                    case UINT1 -> builder.value(((UInt1Vector) vector).getObjectNoOverflow(row));
                    case UINT2 -> builder.value(((UInt2Vector) vector).get(row));
                    case UINT4 -> builder.value(((UInt4Vector) vector).getObjectNoOverflow(row));
                    case UINT8 -> builder.value(((UInt8Vector) vector).getObjectNoOverflow(row));
                    default ->
                            throw new UnsupportedOperationException(
                                    "Unknown minor type of Int type: " + vector.getMinorType());
                }
            }
            case FloatingPoint -> {
                switch (vector.getMinorType()) {
                    case FLOAT4 -> builder.value(((Float4Vector) vector).get(row));
                    case FLOAT8 -> builder.value(((Float8Vector) vector).get(row));
                    default -> builder.value(((FloatingPointVector) vector).getValueAsDouble(row));
                }
            }
            case Utf8, Utf8View, LargeUtf8 -> {
                parseStrTypeColumn(vector, row, builder, false);
            }
            case Binary -> {
                builder.value(((VarBinaryVector) vector).get(row));
            }
            case BinaryView -> {
                builder.value(((ViewVarBinaryVector) vector).get(row));
            }
            case LargeBinary -> {
                builder.value(((LargeVarBinaryVector) vector).get(row));
            }
            case FixedSizeBinary -> {
                builder.value(((FixedSizeBinaryVector) vector).get(row));
            }
            case Bool -> {
                builder.value(((BitVector) vector).get(row) != 0);
            }
            case Decimal -> {
                if (vector.getMinorType() == Types.MinorType.DECIMAL256) {
                    builder.value(((Decimal256Vector) vector).getObject(row));
                } else if (vector.getMinorType() == Types.MinorType.DECIMAL) {
                    builder.value(((DecimalVector) vector).getObject(row));
                } else {
                    throw new UnsupportedOperationException("Unknown minor type of Decimal type");
                }
            }
            case Date -> {
                if (vector.getMinorType() == Types.MinorType.DATEDAY) {
                    builder.value(((DateDayVector) vector).getObject(row));
                } else if (vector.getMinorType() == Types.MinorType.DATEMILLI) {
                    builder.value(((DateMilliVector) vector).getObject(row));
                } else {
                    throw new UnsupportedOperationException("Unknown minor type of Date type");
                }
            }
            case Time -> {
                // Normalize all Time minor types to millisecond resolution since OpenSearch's
                // date field only supports up to millisecond precision. Sub-millisecond units
                // are truncated.
                long timeMillis =
                        switch (vector.getMinorType()) {
                            case TIMESEC -> ((TimeSecVector) vector).get(row) * 1_000L;
                            case TIMEMILLI -> ((TimeMilliVector) vector).get(row);
                            case TIMEMICRO -> ((TimeMicroVector) vector).get(row) / 1_000L;
                            case TIMENANO -> ((TimeNanoVector) vector).get(row) / 1_000_000L;
                            default ->
                                    throw new UnsupportedOperationException(
                                            "Unknown minor type of Time type: "
                                                    + vector.getMinorType());
                        };
                builder.value(timeMillis);
            }
            case Timestamp -> {
                // Normalize all Timestamp minor types (with or without timezone) to millisecond
                // resolution since OpenSearch's date field only supports up to millisecond
                // precision. Sub-millisecond units are truncated.
                long rawTimestamp = ((TimeStampVector) vector).get(row);
                long timestampMillis =
                        switch (vector.getMinorType()) {
                            case TIMESTAMPSEC, TIMESTAMPSECTZ -> rawTimestamp * 1_000L;
                            case TIMESTAMPMILLI, TIMESTAMPMILLITZ -> rawTimestamp;
                            case TIMESTAMPMICRO, TIMESTAMPMICROTZ -> rawTimestamp / 1_000L;
                            case TIMESTAMPNANO, TIMESTAMPNANOTZ -> rawTimestamp / 1_000_000L;
                            default ->
                                    throw new UnsupportedOperationException(
                                            "Unknown minor type of Timestamp type: "
                                                    + vector.getMinorType());
                        };
                builder.value(timestampMillis);
            }
            case Interval -> {
                if (vector.getMinorType() == Types.MinorType.INTERVALDAY) {
                    builder.value(((IntervalDayVector) vector).getObject(row));
                } else if (vector.getMinorType() == Types.MinorType.INTERVALYEAR) {
                    builder.value(((IntervalYearVector) vector).getObject(row));
                } else if (vector.getMinorType() == Types.MinorType.INTERVALMONTHDAYNANO) {
                    builder.value(((IntervalMonthDayNanoVector) vector).getObject(row));
                } else {
                    throw new UnsupportedOperationException("Unknown minor type of Interval type");
                }
            }
            case Duration -> {
                builder.value(((DurationVector) vector).getObject(row));
            }
            default -> {
                throw new UnsupportedOperationException(
                        "Unsupported primitive type for field '"
                                + vector.getField().getName()
                                + "': "
                                + vector.getField().getType().getTypeID());
            }
        }
    }

    private void parseOneComplexColumn(ValueVector vector, int row, XContentBuilder builder)
            throws IOException {
        assert vector.getField().getType().isComplex();
        switch (vector.getField().getType().getTypeID()) {
            case List -> {
                ListVector listVector = (ListVector) vector;
                parseListLikeVectors(
                        listVector.getDataVector(),
                        listVector.getElementStartIndex(row),
                        listVector.getElementEndIndex(row),
                        builder);
            }
            case LargeList -> {
                LargeListVector listVector = (LargeListVector) vector;
                // NOTE: this implementation follows behavior in LargeListVector.getObject(int
                // index) which casting the
                // long index to int when getting object out, as noted in the LargeListVector
                // itself:
                // Currently Arrow in Java doesn't support 64-bit vectors. This class follows the
                // expected behaviour
                // of a LargeList but doesn't actually support allocating a 64-bit vector. It has
                // little use until
                // 64-bit vectors are supported and should be used with caution.
                parseListLikeVectors(
                        listVector.getDataVector(),
                        checkedCastToInt(listVector.getElementStartIndex(row)),
                        checkedCastToInt(listVector.getElementEndIndex(row)),
                        builder);
            }
            case FixedSizeList -> {
                FixedSizeListVector listVector = (FixedSizeListVector) vector;
                parseListLikeVectors(
                        listVector.getDataVector(),
                        listVector.getElementStartIndex(row),
                        listVector.getElementEndIndex(row),
                        builder);
            }
            case ListView -> {
                ListViewVector listViewVector = (ListViewVector) vector;
                parseListLikeVectors(
                        listViewVector.getDataVector(),
                        listViewVector.getElementStartIndex(row),
                        listViewVector.getElementEndIndex(row),
                        builder);
            }
            case LargeListView -> {
                LargeListViewVector listViewVector = (LargeListViewVector) vector;
                // Similarly following LargeListViewVector.getObject, seems the implementation is
                // a bit messed up with long/int conversion
                ArrowBuf offsetBuffer = listViewVector.getOffsetBuffer();
                ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();
                final int start = offsetBuffer.getInt(row * OFFSET_WIDTH);
                final int end = start + sizeBuffer.getInt((row) * SIZE_WIDTH);
                // check integer overflow
                if (end < start) {
                    throw new IllegalArgumentException(
                            "Integer overflow when calculating the end index for large list, start: "
                                    + start
                                    + " end: "
                                    + end);
                }
                parseListLikeVectors(listViewVector.getDataVector(), start, end, builder);
            }
            default -> {
                // Struct, Union, Map, RunEndEncoded not supported yet
                throw new UnsupportedOperationException(
                        "Unsupported complex type for field '"
                                + vector.getField().getName()
                                + "': "
                                + vector.getField().getType().getTypeID());
            }
        }
    }

    /**
     * Recursively parse the list like vectors, this follows implementations of 'getObject' method
     * for all list-like vectors. They are implemented slightly differently on how the index being
     * retrieved and choose of interface implementation, but all of them have stored inner elements
     * in consecutive spaces.
     *
     * @param dataVector the inner data vector of the list type column
     * @param start start index of the inner data
     * @param end end index (exclusive) of the inner data
     */
    private void parseListLikeVectors(
            ValueVector dataVector, int start, int end, XContentBuilder builder)
            throws IOException {
        builder.startArray();
        for (int innerIndex = start; innerIndex < end; innerIndex++) {
            parseOneColumn(dataVector, innerIndex, builder);
        }
        builder.endArray();
    }

    /**
     * Read the sharding key field value and then performing a murmur hash and modulo by total
     * number of shards to decide whether a specific row belong to this shard.
     */
    private boolean belongToThisShard(VectorSchemaRoot vsr, int row) {
        FieldVector fieldVector = vsr.getVector(ingestionConfig.getShardingKey());
        if (fieldVector == null) {
            throw new IllegalArgumentException(
                    "Unknown sharding key field: " + ingestionConfig.getShardingKey());
        }

        if (fieldVector.isNull(row)) {
            throw new IllegalArgumentException(
                    "Sharding key field cannot be null, field: "
                            + ingestionConfig.getShardingKey()
                            + " is null at row: "
                            + nextRead);
        }

        String routingKey =
                switch (fieldVector.getField().getType().getTypeID()) {
                    case Utf8 ->
                            new String(
                                    ((VarCharVector) fieldVector).get(row), StandardCharsets.UTF_8);
                    case Utf8View ->
                            new String(
                                    ((ViewVarCharVector) fieldVector).get(row),
                                    StandardCharsets.UTF_8);
                    case LargeUtf8 ->
                            new String(
                                    ((LargeVarCharVector) fieldVector).get(row),
                                    StandardCharsets.UTF_8);
                    default ->
                            throw new IllegalArgumentException(
                                    "Unsupported sharding key field type: "
                                            + fieldVector.getField().getType().getTypeID()
                                            + " for key field: "
                                            + ingestionConfig.getShardingKey());
                };

        return Math.floorMod(Murmur3HashFunction.hash(routingKey), ingestionConfig.getNumShards())
                == this.shardId;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new ArrowOffset(0);
    }

    /**
     * @return the latest pointer in the shard. The pointer points to the next offset of the last
     *     message in the stream.
     */
    @Override
    public IngestionShardPointer latestPointer() {
        // Currently we don't consume from live stream, so if user reset to the latest pointer it
        // means no event will be
        // consumed
        return new ArrowOffset(rowBound);
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw new UnsupportedOperationException(
                "Arrow dataset doesn't support reset to/from a certain timestamp");
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return ArrowOffset.fromString(offset);
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    /**
     * The lag is measured by max rows - next row to read
     *
     * @param expectedStartPointer the pointer to measure lag from, if it is ahead of the
     *     consumer's own next-read position, the lag is measured from it instead
     */
    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        ArrowOffset arrowStartPointer = (ArrowOffset) expectedStartPointer;
        long currentRow = Math.max(nextRead, arrowStartPointer.getOffset());
        return Math.max(0, rowBound - currentRow);
    }

    @Override
    public void close() throws IOException {
        arrowSource.close();
    }
}
