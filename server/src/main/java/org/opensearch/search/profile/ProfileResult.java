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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class is the internal representation of a profiled Query, corresponding
 * to a single node in the query tree.  It is built after the query has finished executing
 * and is merely a structured representation, rather than the entity that collects the timing
 * profile (see InternalProfiler for that)
 * <p>
 * Each InternalProfileResult has a List of InternalProfileResults, which will contain
 * "children" queries if applicable
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ProfileResult implements Writeable, ToXContentObject {
    protected static final ParseField TYPE = new ParseField("type");
    protected static final ParseField METRICS = new ParseField("important_metrics");
    protected static final ParseField DESCRIPTION = new ParseField("description");
    protected static final ParseField BREAKDOWN = new ParseField("breakdown");
    protected static final ParseField DEBUG = new ParseField("debug");
    protected static final ParseField CHILDREN = new ParseField("children");

    protected final String type;
    protected final String description;
    protected final Map<String, Long> importantMetrics;
    protected final Map<String, Long> breakdown;
    protected final Map<String, Object> debug;
    protected List<ProfileResult> children;

    public ProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        List<ProfileResult> children,
        Map<String, Long> importantMetrics
    ) {
        this.type = type;
        this.description = description;
        this.importantMetrics = Objects.requireNonNull(importantMetrics, "required breakdown argument missing");
        this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
        this.debug = debug == null ? Map.of() : debug;
        this.children = children == null ? List.of() : children;
    }

    /**
     * Read from a stream.
     */
    public ProfileResult(StreamInput in) throws IOException {
        this.type = in.readString();
        this.description = in.readString();
        importantMetrics = in.readMap(StreamInput::readString, StreamInput::readLong);
        breakdown = in.readMap(StreamInput::readString, StreamInput::readLong);
        debug = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        children = in.readList(ProfileResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(description);
        out.writeMap(importantMetrics, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(breakdown, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(debug, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeList(children);
    }

    /**
     * Retrieve the lucene description of this query (e.g. the "explain" text)
     */
    public String getLuceneDescription() {
        return description;
    }

    /**
     * Retrieve the name of the entry (e.g. "TermQuery" or "LongTermsAggregator")
     */
    public String getQueryName() {
        return type;
    }

    /**
     * The important metrics for this node.
     */
    public Map<String, Long> getImportantMetrics() {
        return Collections.unmodifiableMap(importantMetrics);
    }

    /**
     * The breakdown for this node.
     */
    public Map<String, Long> getBreakdown() {
        return Collections.unmodifiableMap(breakdown);
    }

    /**
     * The debug information about the profiled execution.
     */
    public Map<String, Object> getDebugInfo() {
        return Collections.unmodifiableMap(debug);
    }

    /**
     * Returns a list of all profiled children queries
     */
    public List<ProfileResult> getProfiledChildren() {
        return Collections.unmodifiableList(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), getQueryName());
        builder.field(DESCRIPTION.getPreferredName(), getLuceneDescription());
        builder.field(METRICS.getPreferredName(), getImportantMetrics());
        builder.field(BREAKDOWN.getPreferredName(), getBreakdown());
        if (false == getDebugInfo().isEmpty()) {
            builder.field(DEBUG.getPreferredName(), getDebugInfo());
        }

        if (false == children.isEmpty()) {
            builder.startArray(CHILDREN.getPreferredName());
            for (ProfileResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    private static final InstantiatingObjectParser<ProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "profile_result",
            true,
            ProfileResult.class
        );
        parser.declareString(constructorArg(), TYPE);
        parser.declareString(constructorArg(), DESCRIPTION);
        parser.declareObject(constructorArg(), (p, c) -> p.map(), METRICS);
        parser.declareObject(constructorArg(), (p, c) -> p.map(), BREAKDOWN);
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), DEBUG);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> fromXContent(p), CHILDREN);
        PARSER = parser.build();
    }

    public static ProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
