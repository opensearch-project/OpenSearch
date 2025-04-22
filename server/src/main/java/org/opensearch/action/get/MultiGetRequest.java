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

package org.opensearch.action.get;

import org.opensearch.LegacyESVersion;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.CompositeIndicesRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.RealtimeRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Transport request for a multi get.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MultiGetRequest extends ActionRequest
    implements
        Iterable<MultiGetRequest.Item>,
        CompositeIndicesRequest,
        RealtimeRequest,
        ToXContentObject {

    private static final ParseField DOCS = new ParseField("docs");
    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField STORED_FIELDS = new ParseField("stored_fields");
    private static final ParseField SOURCE = new ParseField("_source");

    /**
     * A single get item.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Item implements Writeable, IndicesRequest, ToXContentObject {

        private String index;
        private String id;
        private String routing;
        private String[] storedFields;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private FetchSourceContext fetchSourceContext;

        public Item() {

        }

        public Item(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                in.readOptionalString();
            }
            id = in.readString();
            routing = in.readOptionalString();
            if (in.getVersion().before(LegacyESVersion.V_7_0_0)) {
                in.readOptionalString(); // _parent
            }
            storedFields = in.readOptionalStringArray();
            version = in.readLong();
            versionType = VersionType.fromValue(in.readByte());

            fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        }

        public Item(String index, String id) {
            this.index = index;
            this.id = id;
        }

        public String index() {
            return this.index;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return GetRequest.INDICES_OPTIONS;
        }

        public Item index(String index) {
            this.index = index;
            return this;
        }

        public String id() {
            return this.id;
        }

        /**
         * The routing associated with this document.
         */
        public Item routing(String routing) {
            this.routing = routing;
            return this;
        }

        public String routing() {
            return this.routing;
        }

        public Item storedFields(String... fields) {
            this.storedFields = fields;
            return this;
        }

        public String[] storedFields() {
            return this.storedFields;
        }

        public long version() {
            return version;
        }

        public Item version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Item versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public FetchSourceContext fetchSourceContext() {
            return this.fetchSourceContext;
        }

        /**
         * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
         */
        public Item fetchSourceContext(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            out.writeOptionalString(routing);
            if (out.getVersion().before(LegacyESVersion.V_7_0_0)) {
                out.writeOptionalString(null); // _parent
            }
            out.writeOptionalStringArray(storedFields);
            out.writeLong(version);
            out.writeByte(versionType.getValue());

            out.writeOptionalWriteable(fetchSourceContext);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            builder.field(ID.getPreferredName(), id);
            builder.field(ROUTING.getPreferredName(), routing);
            builder.field(STORED_FIELDS.getPreferredName(), storedFields);
            builder.field(VERSION.getPreferredName(), version);
            builder.field(VERSION_TYPE.getPreferredName(), VersionType.toString(versionType));
            builder.field(SOURCE.getPreferredName(), fetchSourceContext);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;

            Item item = (Item) o;

            if (version != item.version) return false;
            if (fetchSourceContext != null ? !fetchSourceContext.equals(item.fetchSourceContext) : item.fetchSourceContext != null)
                return false;
            if (!Arrays.equals(storedFields, item.storedFields)) return false;
            if (!id.equals(item.id)) return false;
            if (!index.equals(item.index)) return false;
            if (routing != null ? !routing.equals(item.routing) : item.routing != null) return false;
            if (versionType != item.versionType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + id.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (storedFields != null ? Arrays.hashCode(storedFields) : 0);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + (fetchSourceContext != null ? fetchSourceContext.hashCode() : 0);
            return result;
        }

        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

    }

    String preference;
    boolean realtime = true;
    boolean refresh;
    List<Item> items = new ArrayList<>();

    public MultiGetRequest() {}

    public MultiGetRequest(StreamInput in) throws IOException {
        super(in);
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        realtime = in.readBoolean();
        items = in.readList(Item::new);
    }

    public List<Item> getItems() {
        return this.items;
    }

    public MultiGetRequest add(Item item) {
        items.add(item);
        return this;
    }

    public MultiGetRequest add(String index, String id) {
        items.add(new Item(index, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (items.isEmpty()) {
            validationException = ValidateActions.addValidationError("no documents to get", validationException);
        } else {
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                if (item.index() == null) {
                    validationException = ValidateActions.addValidationError("index is missing for doc " + i, validationException);
                }
                if (item.id() == null) {
                    validationException = ValidateActions.addValidationError("id is missing for doc " + i, validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public MultiGetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime;
    }

    @Override
    public MultiGetRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public MultiGetRequest add(
        @Nullable String defaultIndex,
        @Nullable String[] defaultFields,
        @Nullable FetchSourceContext defaultFetchSource,
        @Nullable String defaultRouting,
        XContentParser parser,
        boolean allowExplicitIndex
    ) throws IOException {
        Token token;
        String currentFieldName = null;
        if ((token = parser.nextToken()) != Token.START_OBJECT) {
            final String message = String.format(Locale.ROOT, "unexpected token [%s], expected [%s]", token, Token.START_OBJECT);
            throw new ParsingException(parser.getTokenLocation(), message);
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if ("docs".equals(currentFieldName)) {
                    parseDocuments(parser, this.items, defaultIndex, defaultFields, defaultFetchSource, defaultRouting, allowExplicitIndex);
                } else if ("ids".equals(currentFieldName)) {
                    parseIds(parser, this.items, defaultIndex, defaultFields, defaultFetchSource, defaultRouting);
                } else {
                    final String message = String.format(
                        Locale.ROOT,
                        "unknown key [%s] for a %s, expected [docs] or [ids]",
                        currentFieldName,
                        token
                    );
                    throw new ParsingException(parser.getTokenLocation(), message);
                }
            } else {
                final String message = String.format(
                    Locale.ROOT,
                    "unexpected token [%s], expected [%s] or [%s]",
                    token,
                    Token.FIELD_NAME,
                    Token.START_ARRAY
                );
                throw new ParsingException(parser.getTokenLocation(), message);
            }
        }
        return this;
    }

    private static void parseDocuments(
        XContentParser parser,
        List<Item> items,
        @Nullable String defaultIndex,
        @Nullable String[] defaultFields,
        @Nullable FetchSourceContext defaultFetchSource,
        @Nullable String defaultRouting,
        boolean allowExplicitIndex
    ) throws IOException {
        String currentFieldName = null;
        Token token;
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token != Token.START_OBJECT) {
                throw new IllegalArgumentException("docs array element should include an object");
            }
            String index = defaultIndex;
            String id = null;
            String routing = defaultRouting;
            List<String> storedFields = null;
            long version = Versions.MATCH_ANY;
            VersionType versionType = VersionType.INTERNAL;

            FetchSourceContext fetchSourceContext = FetchSourceContext.FETCH_SOURCE;

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (!allowExplicitIndex) {
                            throw new IllegalArgumentException("explicit index in multi get is not allowed");
                        }
                        index = parser.text();
                    } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        id = parser.text();
                    } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                        routing = parser.text();
                    } else if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unsupported field [fields] used, expected [stored_fields] instead"
                        );
                    } else if (STORED_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        storedFields = new ArrayList<>();
                        storedFields.add(parser.text());
                    } else if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                        version = parser.longValue();
                    } else if (VERSION_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        versionType = VersionType.fromString(parser.text());
                    } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (parser.isBooleanValue()) {
                            fetchSourceContext = new FetchSourceContext(
                                parser.booleanValue(),
                                fetchSourceContext.includes(),
                                fetchSourceContext.excludes()
                            );
                        } else if (token == Token.VALUE_STRING) {
                            fetchSourceContext = new FetchSourceContext(
                                fetchSourceContext.fetchSource(),
                                new String[] { parser.text() },
                                fetchSourceContext.excludes()
                            );
                        } else {
                            throw new OpenSearchParseException("illegal type for _source: [{}]", token);
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse multi get request. unknown field [{}]", currentFieldName);
                    }
                } else if (token == Token.START_ARRAY) {
                    if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unsupported field [fields] used, expected [stored_fields] instead"
                        );
                    } else if (STORED_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        storedFields = new ArrayList<>();
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            storedFields.add(parser.text());
                        }
                    } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        ArrayList<String> includes = new ArrayList<>();
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            includes.add(parser.text());
                        }
                        fetchSourceContext = new FetchSourceContext(
                            fetchSourceContext.fetchSource(),
                            includes.toArray(Strings.EMPTY_ARRAY),
                            fetchSourceContext.excludes()
                        );
                    }

                } else if (token == Token.START_OBJECT) {
                    if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        List<String> currentList = null, includes = null, excludes = null;

                        while ((token = parser.nextToken()) != Token.END_OBJECT) {
                            if (token == Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                                if ("includes".equals(currentFieldName) || "include".equals(currentFieldName)) {
                                    currentList = includes != null ? includes : (includes = new ArrayList<>(2));
                                } else if ("excludes".equals(currentFieldName) || "exclude".equals(currentFieldName)) {
                                    currentList = excludes != null ? excludes : (excludes = new ArrayList<>(2));
                                } else {
                                    throw new OpenSearchParseException("source definition may not contain [{}]", parser.text());
                                }
                            } else if (token == Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                    currentList.add(parser.text());
                                }
                            } else if (token.isValue()) {
                                currentList.add(parser.text());
                            } else {
                                throw new OpenSearchParseException("unexpected token while parsing source settings");
                            }
                        }

                        fetchSourceContext = new FetchSourceContext(
                            fetchSourceContext.fetchSource(),
                            includes == null ? Strings.EMPTY_ARRAY : includes.toArray(new String[0]),
                            excludes == null ? Strings.EMPTY_ARRAY : excludes.toArray(new String[0])
                        );
                    }
                }
            }
            String[] aFields;
            if (storedFields != null) {
                aFields = storedFields.toArray(new String[0]);
            } else {
                aFields = defaultFields;
            }
            items.add(
                new Item(index, id).routing(routing)
                    .storedFields(aFields)
                    .version(version)
                    .versionType(versionType)
                    .fetchSourceContext(fetchSourceContext == FetchSourceContext.FETCH_SOURCE ? defaultFetchSource : fetchSourceContext)
            );
        }
    }

    public static void parseIds(
        XContentParser parser,
        List<Item> items,
        @Nullable String defaultIndex,
        @Nullable String[] defaultFields,
        @Nullable FetchSourceContext defaultFetchSource,
        @Nullable String defaultRouting
    ) throws IOException {
        Token token;
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (!token.isValue()) {
                throw new IllegalArgumentException("ids array element should only contain ids");
            }
            items.add(
                new Item(defaultIndex, parser.text()).storedFields(defaultFields)
                    .fetchSourceContext(defaultFetchSource)
                    .routing(defaultRouting)
            );
        }
    }

    @Override
    public Iterator<Item> iterator() {
        return Collections.unmodifiableCollection(items).iterator();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        out.writeBoolean(realtime);
        out.writeList(items);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DOCS.getPreferredName());
        for (Item item : items) {
            builder.value(item);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "MultiGetRequest{"
            + "preference='"
            + preference
            + '\''
            + ", realtime="
            + realtime
            + ", refresh="
            + refresh
            + ", items="
            + items
            + '}';
    }

}
