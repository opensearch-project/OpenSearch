/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.pipeline.PipelineConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * transport response for getting a search pipeline
 *
 * @opensearch.internal
 */
public class GetSearchPipelineResponse extends ActionResponse implements StatusToXContentObject {

    private final List<PipelineConfiguration> pipelines;

    public GetSearchPipelineResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        pipelines = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            pipelines.add(PipelineConfiguration.readFrom(in));
        }

    }

    public GetSearchPipelineResponse(List<PipelineConfiguration> pipelines) {
        this.pipelines = pipelines;
    }

    public List<PipelineConfiguration> pipelines() {
        return Collections.unmodifiableList(pipelines);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (PipelineConfiguration pipeline : pipelines) {
            builder.field(pipeline.getId(), pipeline.getConfigAsMap());
        }
        builder.endObject();
        return builder;
    }

    /**
     *
     * @param parser the parser for the XContent that contains the serialized GetPipelineResponse.
     * @return an instance of GetPipelineResponse read from the parser
     * @throws IOException If the parsing fails
     */
    public static GetSearchPipelineResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        List<PipelineConfiguration> pipelines = new ArrayList<>();
        while (parser.nextToken().equals(XContentParser.Token.FIELD_NAME)) {
            String pipelineId = parser.currentName();
            parser.nextToken();
            try (XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent())) {
                contentBuilder.generator().copyCurrentStructure(parser);
                PipelineConfiguration pipeline = new PipelineConfiguration(
                    pipelineId,
                    BytesReference.bytes(contentBuilder),
                    contentBuilder.contentType()
                );
                pipelines.add(pipeline);
            }
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
        return new GetSearchPipelineResponse(pipelines);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pipelines.size());
        for (PipelineConfiguration pipeline : pipelines) {
            pipeline.writeTo(out);
        }
    }

    public boolean isFound() {
        return !pipelines.isEmpty();
    }

    @Override
    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSearchPipelineResponse otherResponse = (GetSearchPipelineResponse) o;
        if (pipelines == null) {
            return otherResponse.pipelines == null;
        } else if (otherResponse.pipelines == null) {
            return false;
        }
        // Convert to a map to ignore order;
        return toMap(pipelines).equals(toMap(otherResponse.pipelines));
    }

    private static Map<String, PipelineConfiguration> toMap(List<PipelineConfiguration> pipelines) {
        return pipelines.stream().collect(Collectors.toMap(PipelineConfiguration::getId, p -> p));
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (PipelineConfiguration pipeline : pipelines) {
            // We only take the sum here to ensure that the order does not matter.
            result += (pipeline == null ? 0 : pipeline.hashCode());
        }
        return result;
    }
}
