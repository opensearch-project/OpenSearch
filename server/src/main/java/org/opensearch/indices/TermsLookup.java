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

package org.opensearch.indices;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Encapsulates the parameters needed to fetch terms.
 *
 * @opensearch.internal
 */
public class TermsLookup implements Writeable, ToXContentFragment {

    private final String index;
    private final String id;
    private final String path;
    private String routing;

    public TermsLookup(String index, String id, String path) {
        if (id == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the id.");
        }
        if (path == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the path.");
        }
        if (index == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the index.");
        }
        this.index = index;
        this.id = id;
        this.path = path;
    }

    /**
     * Read from a stream.
     */
    public TermsLookup(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalString();
        }
        id = in.readString();
        path = in.readString();
        index = in.readString();
        routing = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            store = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeString(path);
        out.writeString(index);
        out.writeOptionalString(routing);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeBoolean(store);
        }
    }

    public String index() {
        return index;
    }

    public String id() {
        return id;
    }

    public String path() {
        return path;
    }

    public String routing() {
        return routing;
    }

    public TermsLookup routing(String routing) {
        this.routing = routing;
        return this;
    }

    private boolean store;

    public boolean store() {
        return store;
    }

    public TermsLookup store(boolean store) {
        this.store = store;
        return this;
    }

    private static final ConstructingObjectParser<TermsLookup, Void> PARSER = new ConstructingObjectParser<>("terms_lookup", args -> {
        String index = (String) args[0];
        String id = (String) args[1];
        String path = (String) args[2];
        return new TermsLookup(index, id, path);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("path"));
        PARSER.declareString(TermsLookup::routing, new ParseField("routing"));
        PARSER.declareBoolean(TermsLookup::store, new ParseField("store"));
    }

    public static TermsLookup parseTermsLookup(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return index + "/" + id + "/" + path;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", index);
        builder.field("id", id);
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        if (store) {
            builder.field("store", true);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, path, routing, store);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TermsLookup other = (TermsLookup) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(id, other.id)
            && Objects.equals(path, other.path)
            && Objects.equals(routing, other.routing)
            && Objects.equals(store, other.store);
    }
}
