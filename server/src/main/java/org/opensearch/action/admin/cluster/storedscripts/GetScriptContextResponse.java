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

package org.opensearch.action.admin.cluster.storedscripts;

import org.opensearch.action.ActionResponse;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.ScriptContextInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transport context response for getting stored scripts
 *
 * @opensearch.internal
 */
public class GetScriptContextResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField CONTEXTS = new ParseField("contexts");
    final Map<String, ScriptContextInfo> contexts;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetScriptContextResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_script_context",
        true,
        (a) -> {
            Map<String, ScriptContextInfo> contexts = ((List<ScriptContextInfo>) a[0]).stream()
                .collect(Collectors.toMap(ScriptContextInfo::getName, c -> c));
            return new GetScriptContextResponse(contexts);
        }
    );

    static {
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (parser, ctx) -> ScriptContextInfo.PARSER.apply(parser, ctx),
            CONTEXTS
        );
    }

    GetScriptContextResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readInt();
        HashMap<String, ScriptContextInfo> contexts = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ScriptContextInfo info = new ScriptContextInfo(in);
            contexts.put(info.name, info);
        }
        this.contexts = Collections.unmodifiableMap(contexts);
    }

    // TransportAction constructor
    GetScriptContextResponse(Set<ScriptContextInfo> contexts) {
        this.contexts = Collections.unmodifiableMap(
            contexts.stream().collect(Collectors.toMap(ScriptContextInfo::getName, Function.identity()))
        );
    }

    // Parser constructor
    private GetScriptContextResponse(Map<String, ScriptContextInfo> contexts) {
        this.contexts = Collections.unmodifiableMap(contexts);
    }

    private List<ScriptContextInfo> byName() {
        return contexts.values().stream().sorted(Comparator.comparing(ScriptContextInfo::getName)).collect(Collectors.toList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(contexts.size());
        for (ScriptContextInfo context : contexts.values()) {
            context.writeTo(out);
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray(CONTEXTS.getPreferredName());
        for (ScriptContextInfo context : byName()) {
            context.toXContent(builder, params);
        }
        builder.endArray().endObject(); // CONTEXTS
        return builder;
    }

    public static GetScriptContextResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetScriptContextResponse that = (GetScriptContextResponse) o;
        return contexts.equals(that.contexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contexts);
    }
}
