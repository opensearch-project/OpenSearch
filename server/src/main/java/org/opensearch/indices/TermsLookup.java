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
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Encapsulates the parameters needed to fetch terms.
 *
 * @opensearch.internal
 */
public class TermsLookup implements Writeable, ToXContentFragment {

    private final String index;
    private String id;
    private final String path;
    private String routing;
    private QueryBuilder query;

    public TermsLookup(String index, String id, String path) {
        this(index, id, path, null);
    }

    public TermsLookup(String index, String id, String path, QueryBuilder query) {
        if (index == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] index cannot be null or empty for TermsLookup");
        }
        if (path == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] path cannot be null or empty for TermsLookup");
        }
        if (id == null && query == null) {
            throw new IllegalArgumentException(
                "[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying either the id or the query."
            );
        }
        if (id != null && query != null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.");

        }

        this.index = index;
        this.id = id;
        this.path = path;
        this.query = query;
    }

    public String index() {
        return index;
    }

    public String id() {
        return id;
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
        if (in.getVersion().onOrAfter(Version.V_3_2_0)) {
            query = in.readOptionalWriteable(inStream -> inStream.readNamedWriteable(QueryBuilder.class));
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
        if (out.getVersion().onOrAfter(Version.V_3_2_0)) {
            out.writeOptionalWriteable(query);
        }
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

    public QueryBuilder query() {
        return query;
    }

    public TermsLookup query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    public void setQuery(QueryBuilder query) {
        if (this.id != null && query != null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.");
        }
        this.query = query;
    }

    public TermsLookup id(String id) {
        if (this.query != null && id != null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.");
        }
        this.id = id;
        return this;
    }

    private static final ConstructingObjectParser<TermsLookup, Void> PARSER = new ConstructingObjectParser<>("terms_lookup", args -> {
        String index = (String) args[0];
        String id = (String) args[1]; // Optional id or query but not both
        String path = (String) args[2];
        QueryBuilder query = (QueryBuilder) args[3]; // Optional id or query but not both

        // Validation: Either id or query must be present, but not both
        if (id == null && query == null) {
            throw new IllegalArgumentException(
                "[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying either the id or the query."
            );
        }
        if (id != null && query != null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.");
        }
        return new TermsLookup(index, id, path, query);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("index")); // Required
        PARSER.declareString(optionalConstructorArg(), new ParseField("id")); // Optional
        PARSER.declareString(constructorArg(), new ParseField("path")); // Required
        PARSER.declareObject(optionalConstructorArg(), (parser, context) -> {
            try {
                return AbstractQueryBuilder.parseInnerQueryBuilder(parser); // Parse query if provided
            } catch (IOException e) {
                throw new RuntimeException("Error parsing inner query builder", e);
            }
        }, new ParseField("query")); // Optional
        PARSER.declareString(TermsLookup::routing, new ParseField("routing")); // Optional
        PARSER.declareBoolean(TermsLookup::store, new ParseField("store")); // Optional
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
        if (id != null) {
            builder.field("id", id);
        }
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        if (query != null) {
            builder.field("query");
            query.toXContent(builder, params);
        }
        if (store) {
            builder.field("store", true);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, path, routing, store, query);
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
            && Objects.equals(store, other.store)
            && Objects.equals(query, other.query);
    }
}
