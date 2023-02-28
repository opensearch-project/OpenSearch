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

package org.opensearch.action.ingest;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionResponse;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * transport response for simulating a pipeline
 *
 * @opensearch.internal
 */
public class SimulatePipelineResponse extends ActionResponse implements ToXContentObject {
    private String pipelineId;
    private boolean verbose;
    private List<SimulateDocumentResult> results;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SimulatePipelineResponse, Void> PARSER = new ConstructingObjectParser<>(
        "simulate_pipeline_response",
        true,
        a -> {
            List<SimulateDocumentResult> results = (List<SimulateDocumentResult>) a[0];
            boolean verbose = false;
            if (results.size() > 0) {
                if (results.get(0) instanceof SimulateDocumentVerboseResult) {
                    verbose = true;
                }
            }
            return new SimulatePipelineResponse(null, verbose, results);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), (parser, context) -> {
            Token token = parser.currentToken();
            ensureExpectedToken(Token.START_OBJECT, token, parser);
            SimulateDocumentResult result = null;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                ensureExpectedToken(Token.FIELD_NAME, token, parser);
                String fieldName = parser.currentName();
                token = parser.nextToken();
                if (token == Token.START_ARRAY) {
                    if (fieldName.equals(SimulateDocumentVerboseResult.PROCESSOR_RESULT_FIELD)) {
                        List<SimulateProcessorResult> results = new ArrayList<>();
                        while ((token = parser.nextToken()) == Token.START_OBJECT) {
                            results.add(SimulateProcessorResult.fromXContent(parser));
                        }
                        ensureExpectedToken(Token.END_ARRAY, token, parser);
                        result = new SimulateDocumentVerboseResult(results);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token.equals(Token.START_OBJECT)) {
                    switch (fieldName) {
                        case WriteableIngestDocument.DOC_FIELD:
                            result = new SimulateDocumentBaseResult(
                                WriteableIngestDocument.INGEST_DOC_PARSER.apply(parser, null).getIngestDocument()
                            );
                            break;
                        case "error":
                            result = new SimulateDocumentBaseResult(OpenSearchException.fromXContent(parser));
                            break;
                        default:
                            parser.skipChildren();
                            break;
                    }
                } // else it is a value skip it
            }
            assert result != null;
            return result;
        }, new ParseField(Fields.DOCUMENTS));
    }

    public SimulatePipelineResponse(StreamInput in) throws IOException {
        super(in);
        this.pipelineId = in.readOptionalString();
        boolean verbose = in.readBoolean();
        int responsesLength = in.readVInt();
        results = new ArrayList<>();
        for (int i = 0; i < responsesLength; i++) {
            SimulateDocumentResult simulateDocumentResult;
            if (verbose) {
                simulateDocumentResult = new SimulateDocumentVerboseResult(in);
            } else {
                simulateDocumentResult = new SimulateDocumentBaseResult(in);
            }
            results.add(simulateDocumentResult);
        }
    }

    public SimulatePipelineResponse(String pipelineId, boolean verbose, List<SimulateDocumentResult> responses) {
        this.pipelineId = pipelineId;
        this.verbose = verbose;
        this.results = Collections.unmodifiableList(responses);
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public List<SimulateDocumentResult> getResults() {
        return results;
    }

    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(pipelineId);
        out.writeBoolean(verbose);
        out.writeVInt(results.size());
        for (SimulateDocumentResult response : results) {
            response.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.DOCUMENTS);
        for (SimulateDocumentResult response : results) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SimulatePipelineResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String DOCUMENTS = "docs";
    }
}
