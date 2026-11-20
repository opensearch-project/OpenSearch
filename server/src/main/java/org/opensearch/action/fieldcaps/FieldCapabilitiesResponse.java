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

package org.opensearch.action.fieldcaps;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Response for {@link FieldCapabilitiesRequest} requests.
 *
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FieldCapabilitiesResponse extends ActionResponse implements ToXContentObject {
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");
    private static final ParseField FAILURE_INDEX_FIELD = new ParseField("index");
    private static final ParseField FAILURE_REASON_FIELD = new ParseField("reason");

    private final String[] indices;
    private final Map<String, Map<String, FieldCapabilities>> responseMap;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;
    private final Map<String, Exception> failures;

    public FieldCapabilitiesResponse(String[] indices, Map<String, Map<String, FieldCapabilities>> responseMap) {
        this(indices, responseMap, Collections.emptyMap());
    }

    public FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> responseMap,
        Map<String, Exception> failures
    ) {
        this(indices, responseMap, Collections.emptyList(), failures);
    }

    FieldCapabilitiesResponse(List<FieldCapabilitiesIndexResponse> indexResponses) {
        this(indexResponses, Collections.emptyMap());
    }

    FieldCapabilitiesResponse(List<FieldCapabilitiesIndexResponse> indexResponses, Map<String, Exception> failures) {
        this(Strings.EMPTY_ARRAY, Collections.emptyMap(), indexResponses, failures);
    }

    private FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> responseMap,
        List<FieldCapabilitiesIndexResponse> indexResponses,
        Map<String, Exception> failures
    ) {
        this.responseMap = Objects.requireNonNull(responseMap);
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.failures = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(failures)));
        this.indices = indices;
    }

    public FieldCapabilitiesResponse(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        this.responseMap = in.readMap(StreamInput::readString, FieldCapabilitiesResponse::readField);
        indexResponses = in.readList(FieldCapabilitiesIndexResponse::new);
        if (in.getVersion().onOrAfter(Version.V_3_10_0)) {
            failures = Collections.unmodifiableMap(new TreeMap<>(in.readMap(StreamInput::readString, StreamInput::readException)));
        } else {
            failures = Collections.emptyMap();
        }
    }

    /**
     * Used for serialization
     */
    FieldCapabilitiesResponse() {
        this(Strings.EMPTY_ARRAY, Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    }

    /**
     * Get the concrete list of indices that were requested.
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * Get the field capabilities map.
     */
    public Map<String, Map<String, FieldCapabilities>> get() {
        return responseMap;
    }

    /**
     * Get the indices that could not be checked, keyed by index name, with the reason. Such an index is
     * absent from {@link #getIndices()} because its capabilities, or whether it matches the index filter,
     * are unknown, not because it was found not to match. For a remote cluster that could not be reached,
     * the key is the index expression requested from it, such as {@code remote:logs-*}. Responses compare
     * failures by index only, since exceptions have no value equality.
     */
    public Map<String, Exception> getFailures() {
        return failures;
    }

    /**
     * Returns the actual per-index field caps responses
     */
    List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    /**
     *
     * Get the field capabilities per type for the provided {@code field}.
     */
    public Map<String, FieldCapabilities> getField(String field) {
        return responseMap.get(field);
    }

    private static Map<String, FieldCapabilities> readField(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, FieldCapabilities::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeMap(responseMap, StreamOutput::writeString, FieldCapabilitiesResponse::writeField);
        out.writeList(indexResponses);
        if (out.getVersion().onOrAfter(Version.V_3_10_0)) {
            out.writeMap(failures, StreamOutput::writeString, StreamOutput::writeException);
        }
    }

    private static void writeField(StreamOutput out, Map<String, FieldCapabilities> map) throws IOException {
        out.writeMap(map, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (indexResponses.size() > 0) {
            throw new IllegalStateException("cannot serialize non-merged response");
        }
        builder.startObject();
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(FIELDS_FIELD.getPreferredName(), responseMap);
        if (failures.isEmpty() == false) {
            builder.startArray(FAILURES_FIELD.getPreferredName());
            for (Map.Entry<String, Exception> failure : failures.entrySet()) {
                builder.startObject();
                builder.field(FAILURE_INDEX_FIELD.getPreferredName(), failure.getKey());
                builder.startObject(FAILURE_REASON_FIELD.getPreferredName());
                OpenSearchException.generateThrowableXContent(builder, params, failure.getValue());
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    public static FieldCapabilitiesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilitiesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "field_capabilities_response",
        true,
        a -> {
            List<String> indices = a[0] == null ? Collections.emptyList() : (List<String>) a[0];
            List<Tuple<String, Exception>> failures = a[2] == null ? Collections.emptyList() : (List<Tuple<String, Exception>>) a[2];
            return new FieldCapabilitiesResponse(
                indices.stream().toArray(String[]::new),
                ((List<Tuple<String, Map<String, FieldCapabilities>>>) a[1]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)),
                failures.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))
            );
        }
    );

    private static final ConstructingObjectParser<Tuple<String, Exception>, Void> FAILURE_PARSER = new ConstructingObjectParser<>(
        "field_capabilities_failure",
        true,
        a -> new Tuple<>((String) a[0], (Exception) a[1])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            Map<String, FieldCapabilities> typeToCapabilities = parseTypeToCapabilities(p, n);
            return new Tuple<>(n, typeToCapabilities);
        }, FIELDS_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), FAILURE_PARSER, FAILURES_FIELD);
        FAILURE_PARSER.declareString(ConstructingObjectParser.constructorArg(), FAILURE_INDEX_FIELD);
        FAILURE_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> OpenSearchException.fromXContent(p),
            FAILURE_REASON_FIELD
        );
    }

    private static Map<String, FieldCapabilities> parseTypeToCapabilities(XContentParser parser, String name) throws IOException {
        Map<String, FieldCapabilities> typeToCapabilities = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String type = parser.currentName();
            FieldCapabilities capabilities = FieldCapabilities.fromXContent(name, parser);
            typeToCapabilities.put(type, capabilities);
        }
        return typeToCapabilities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesResponse that = (FieldCapabilitiesResponse) o;
        return Arrays.equals(indices, that.indices)
            && Objects.equals(responseMap, that.responseMap)
            && Objects.equals(indexResponses, that.indexResponses)
            && Objects.equals(failures.keySet(), that.failures.keySet());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(responseMap, indexResponses, failures.keySet());
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
