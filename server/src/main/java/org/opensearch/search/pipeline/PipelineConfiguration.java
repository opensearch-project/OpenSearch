/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * TODO: Copied verbatim from {@link org.opensearch.ingest.PipelineConfiguration}.
 *
 * See if we can refactor into a common class. I suspect not, just because this one will hold
 */
public class PipelineConfiguration extends AbstractDiffable<PipelineConfiguration> implements ToXContentObject {
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
        "pipeline_config",
        true,
        PipelineConfiguration.Builder::new
    );
    static {
        PARSER.declareString(PipelineConfiguration.Builder::setId, new ParseField("id"));
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(parser);
            builder.setConfig(BytesReference.bytes(contentBuilder), contentBuilder.contentType());
        }, new ParseField("config"), ObjectParser.ValueType.OBJECT);

    }

    public static ContextParser<Void, PipelineConfiguration> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }

    private static class Builder {

        private String id;
        private BytesReference config;
        private MediaType mediaType;

        void setId(String id) {
            this.id = id;
        }

        void setConfig(BytesReference config, MediaType mediaType) {
            if (mediaType instanceof XContentType == false) {
                throw new IllegalArgumentException("PipelineConfiguration does not support media type [" + mediaType.getClass() + "]");
            }
            this.config = config;
            this.mediaType = mediaType;
        }

        PipelineConfiguration build() {
            return new PipelineConfiguration(id, config, mediaType);
        }
    }

    private final String id;
    // Store config as bytes reference, because the config is only used when the pipeline store reads the cluster state
    // and the way the map of maps config is read requires a deep copy (it removes instead of gets entries to check for unused options)
    // also the get pipeline api just directly returns this to the caller
    private final BytesReference config;
    private final MediaType mediaType;

    public PipelineConfiguration(String id, BytesReference config, MediaType mediaType) {
        this.id = Objects.requireNonNull(id);
        this.config = Objects.requireNonNull(config);
        this.mediaType = Objects.requireNonNull(mediaType);
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getConfigAsMap() {
        return XContentHelper.convertToMap(config, true, mediaType).v2();
    }

    // pkg-private for tests
    MediaType getMediaType() {
        return mediaType;
    }

    // pkg-private for tests
    BytesReference getConfig() {
        return config;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("config", getConfigAsMap());
        builder.endObject();
        return builder;
    }

    public static PipelineConfiguration readFrom(StreamInput in) throws IOException {
        return new PipelineConfiguration(
            in.readString(),
            in.readBytesReference(),
            in.getVersion().onOrAfter(Version.V_2_10_0) ? in.readMediaType() : in.readEnum(XContentType.class)
        );
    }

    public static Diff<PipelineConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(PipelineConfiguration::readFrom, in);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBytesReference(config);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            mediaType.writeTo(out);
        } else {
            out.writeEnum((XContentType) mediaType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PipelineConfiguration that = (PipelineConfiguration) o;

        if (!id.equals(that.id)) return false;
        return getConfigAsMap().equals(that.getConfigAsMap());

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + getConfigAsMap().hashCode();
        return result;
    }
}
