/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Primary DataStream class
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class DataStream extends AbstractDiffable<DataStream> implements ToXContentObject {

    public static final String BACKING_INDEX_PREFIX = ".ds-";
    public static final String TIMESERIES_FIELDNAME = "@timestamp";
    public static final Comparator<LeafReader> TIMESERIES_LEAF_SORTER = Comparator.comparingLong((LeafReader r) -> {
        try {
            PointValues points = r.getPointValues(TIMESERIES_FIELDNAME);
            if (points != null) {
                // could be a multipoint (probably not) but get the maximum time value anyway
                byte[] sortValue = points.getMaxPackedValue();
                // decode the first dimension because this should not be a multi dimension field
                // it's a bug in the date field if it is
                return LongPoint.decodeDimension(sortValue, 0);
            } else {
                // segment does not have a timestamp field, just return the minimum value
                return Long.MIN_VALUE;
            }
        } catch (IOException e) {
            throw new OpenSearchException("Not a timeseries Index! Field [{}] not found!", TIMESERIES_FIELDNAME);
        }
    }).reversed();

    private final String name;
    private final TimestampField timeStampField;
    private final List<Index> indices;
    private final long generation;

    public DataStream(String name, TimestampField timeStampField, List<Index> indices, long generation) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = Collections.unmodifiableList(indices);
        this.generation = generation;
        assert indices.size() > 0;
        assert indices.get(indices.size() - 1).getName().equals(getDefaultBackingIndexName(name, generation));
    }

    public DataStream(String name, TimestampField timeStampField, List<Index> indices) {
        this(name, timeStampField, indices, indices.size());
    }

    public String getName() {
        return name;
    }

    public TimestampField getTimeStampField() {
        return timeStampField;
    }

    public List<Index> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    /**
     * Performs a rollover on a {@code DataStream} instance and returns a new instance containing
     * the updated list of backing indices and incremented generation.
     *
     * @param newWriteIndex the new write backing index. Must conform to the naming convention for
     *                      backing indices on data streams. See {@link #getDefaultBackingIndexName}.
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rollover(Index newWriteIndex) {
        assert newWriteIndex.getName().equals(getDefaultBackingIndexName(name, generation + 1));
        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(newWriteIndex);
        return new DataStream(name, timeStampField, backingIndices, generation + 1);
    }

    /**
     * Removes the specified backing index and returns a new {@code DataStream} instance with
     * the remaining backing indices.
     *
     * @param index the backing index to remove
     * @return new {@code DataStream} instance with the remaining backing indices
     */
    public DataStream removeBackingIndex(Index index) {
        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.remove(index);
        assert backingIndices.size() == indices.size() - 1;
        return new DataStream(name, timeStampField, backingIndices, generation);
    }

    /**
     * Replaces the specified backing index with a new index and returns a new {@code DataStream} instance with
     * the modified backing indices. An {@code IllegalArgumentException} is thrown if the index to be replaced
     * is not a backing index for this data stream or if it is the {@code DataStream}'s write index.
     *
     * @param existingBackingIndex the backing index to be replaced
     * @param newBackingIndex      the new index that will be part of the {@code DataStream}
     * @return new {@code DataStream} instance with backing indices that contain replacement index instead of the specified
     * existing index.
     */
    public DataStream replaceBackingIndex(Index existingBackingIndex, Index newBackingIndex) {
        List<Index> backingIndices = new ArrayList<>(indices);
        int backingIndexPosition = backingIndices.indexOf(existingBackingIndex);
        if (backingIndexPosition == -1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "index [%s] is not part of data stream [%s] ", existingBackingIndex.getName(), name)
            );
        }
        if (generation == (backingIndexPosition + 1)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot replace backing index [%s] of data stream [%s] because " + "it is the write index",
                    existingBackingIndex.getName(),
                    name
                )
            );
        }
        backingIndices.set(backingIndexPosition, newBackingIndex);
        return new DataStream(name, timeStampField, backingIndices, generation);
    }

    /**
     * Generates the name of the index that conforms to the default naming convention for backing indices
     * on data streams given the specified data stream name and generation.
     *
     * @param dataStreamName name of the data stream
     * @param generation generation of the data stream
     * @return backing index name
     */
    public static String getDefaultBackingIndexName(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, BACKING_INDEX_PREFIX + "%s-%06d", dataStreamName, generation);
    }

    public DataStream(StreamInput in) throws IOException {
        this(in.readString(), new TimestampField(in), in.readList(Index::new), in.readVLong());
    }

    public static Diff<DataStream> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStream::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        timeStampField.writeTo(out);
        out.writeList(indices);
        out.writeVLong(generation);
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>(
        "data_stream",
        args -> new DataStream((String) args[0], (TimestampField) args[1], (List<Index>) args[2], (Long) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimestampField.PARSER, TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Index.fromXContent(p), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(TIMESTAMP_FIELD_FIELD.getPreferredName(), timeStampField);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(GENERATION_FIELD.getPreferredName(), generation);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return name.equals(that.name)
            && timeStampField.equals(that.timeStampField)
            && indices.equals(that.indices)
            && generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeStampField, indices, generation);
    }

    /**
     * A timestamp field.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class TimestampField implements Writeable, ToXContentObject {

        static ParseField NAME_FIELD = new ParseField("name");

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<TimestampField, Void> PARSER = new ConstructingObjectParser<>(
            "timestamp_field",
            args -> new TimestampField((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        }

        private final String name;

        public TimestampField(String name) {
            this.name = name;
        }

        public TimestampField(StreamInput in) throws IOException {
            this.name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.endObject();
            return builder;
        }

        public Map<String, Object> toMap() {
            return Collections.singletonMap(NAME_FIELD.getPreferredName(), name);
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampField that = (TimestampField) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
