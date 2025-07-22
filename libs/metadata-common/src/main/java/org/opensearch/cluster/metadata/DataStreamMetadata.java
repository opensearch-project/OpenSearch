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

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.ComposableIndexTemplateMetadata.MINIMMAL_SUPPORTED_VERSION;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link DataStream}s and their names.
 *
 * @opensearch.internal
 */
public class DataStreamMetadata implements Metadata.Custom {

    public static final String TYPE = "data_stream";
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        a -> new DataStreamMetadata((Map<String, DataStream>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataStream> dataStreams = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataStreams.put(name, DataStream.fromXContent(p));
            }
            return dataStreams;
        }, DATA_STREAM);
    }

    private final Map<String, DataStream> dataStreams;

    public DataStreamMetadata(Map<String, DataStream> dataStreams) {
        this.dataStreams = dataStreams;
    }

    public DataStreamMetadata(StreamInput in) throws IOException {
        this.dataStreams = in.readMap(StreamInput::readString, DataStream::new);
    }

    public Map<String, DataStream> dataStreams() {
        return this.dataStreams;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new DataStreamMetadata.DataStreamMetadataDiff((DataStreamMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new DataStreamMetadata.DataStreamMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MINIMMAL_SUPPORTED_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.dataStreams, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static DataStreamMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DATA_STREAM.getPreferredName());
        for (Map.Entry<String, DataStream> dataStream : dataStreams.entrySet()) {
            builder.field(dataStream.getKey(), dataStream.getValue());
        }
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataStreams);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamMetadata other = (DataStreamMetadata) obj;
        return Objects.equals(this.dataStreams, other.dataStreams);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Builder of data stream metadata.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private final Map<String, DataStream> dataStreams = new HashMap<>();

        public Builder putDataStream(DataStream dataStream) {
            dataStreams.put(dataStream.getName(), dataStream);
            return this;
        }

        public DataStreamMetadata build() {
            return new DataStreamMetadata(dataStreams);
        }
    }

    /**
     * A diff between data stream metadata.
     *
     * @opensearch.internal
     */
    static class DataStreamMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, DataStream>> dataStreamDiff;

        DataStreamMetadataDiff(DataStreamMetadata before, DataStreamMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.dataStreams, after.dataStreams, DiffableUtils.getStringKeySerializer());
        }

        DataStreamMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                DataStream::new,
                DataStream::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DataStreamMetadata(dataStreamDiff.apply(((DataStreamMetadata) part).dataStreams));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
