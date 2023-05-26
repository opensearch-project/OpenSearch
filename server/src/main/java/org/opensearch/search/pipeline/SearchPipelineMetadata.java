/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the search pipelines that are available in the cluster
 *
 * @opensearch.internal
 */
public class SearchPipelineMetadata implements Metadata.Custom {
    public static final String TYPE = "search_pipeline";

    private static final ParseField PIPELINES_FIELD = new ParseField("pipeline");
    private static final ObjectParser<List<PipelineConfiguration>, Void> SEARCH_PIPELINE_METADATA_PARSER = new ObjectParser<>(
        "search_pipeline_metadata",
        ArrayList::new
    );
    static {
        SEARCH_PIPELINE_METADATA_PARSER.declareObjectArray(List::addAll, PipelineConfiguration.getParser(), PIPELINES_FIELD);
    }
    // Mapping from pipeline ID to each pipeline's configuration.
    private final Map<String, PipelineConfiguration> pipelines;

    public SearchPipelineMetadata(Map<String, PipelineConfiguration> pipelines) {
        this.pipelines = Collections.unmodifiableMap(pipelines);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public Map<String, PipelineConfiguration> getPipelines() {
        return pipelines;
    }

    public SearchPipelineMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, PipelineConfiguration> pipelines = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            PipelineConfiguration pipeline = PipelineConfiguration.readFrom(in);
            pipelines.put(pipeline.getId(), pipeline);
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pipelines.size());
        for (PipelineConfiguration pipeline : pipelines.values()) {
            pipeline.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(PIPELINES_FIELD.getPreferredName());
        for (PipelineConfiguration pipeline : pipelines.values()) {
            pipeline.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static SearchPipelineMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        List<PipelineConfiguration> configs = SEARCH_PIPELINE_METADATA_PARSER.parse(parser, null);
        for (PipelineConfiguration pipeline : configs) {
            pipelines.put(pipeline.getId(), pipeline);
        }
        return new SearchPipelineMetadata(pipelines);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new SearchPipelineMetadataDiff((SearchPipelineMetadata) previousState, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new SearchPipelineMetadataDiff(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchPipelineMetadata that = (SearchPipelineMetadata) o;
        return pipelines.equals(that.pipelines);
    }

    @Override
    public int hashCode() {
        return pipelines.hashCode();
    }

    static class SearchPipelineMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, PipelineConfiguration>> pipelines;

        public SearchPipelineMetadataDiff(SearchPipelineMetadata before, SearchPipelineMetadata after) {
            this.pipelines = DiffableUtils.diff(before.pipelines, after.pipelines, DiffableUtils.getStringKeySerializer());
        }

        public SearchPipelineMetadataDiff(StreamInput in) throws IOException {
            this.pipelines = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                PipelineConfiguration::readFrom,
                PipelineConfiguration::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new SearchPipelineMetadata(pipelines.apply(((SearchPipelineMetadata) part).pipelines));
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pipelines.writeTo(out);
        }
    }
}
