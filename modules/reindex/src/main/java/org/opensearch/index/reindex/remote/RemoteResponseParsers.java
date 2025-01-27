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

package org.opensearch.index.reindex.remote;

import org.apache.lucene.search.TotalHits;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ObjectParser.ValueType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.ScrollableHitSource.BasicHit;
import org.opensearch.index.reindex.ScrollableHitSource.Hit;
import org.opensearch.index.reindex.ScrollableHitSource.Response;
import org.opensearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.opensearch.search.SearchHits;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parsers to convert the response from the remote host into objects useful for {@link RemoteScrollableHitSource}.
 */
final class RemoteResponseParsers {
    private RemoteResponseParsers() {}

    /**
     * Parser for an individual {@code hit} element.
     */
    public static final ConstructingObjectParser<BasicHit, MediaType> HIT_PARSER = new ConstructingObjectParser<>("hit", true, a -> {
        int i = 0;
        String index = (String) a[i++];
        String id = (String) a[i++];
        Long version = (Long) a[i++];
        return new BasicHit(index, id, version == null ? -1 : version);
    });
    static {
        HIT_PARSER.declareString(constructorArg(), new ParseField("_index"));
        HIT_PARSER.declareString(constructorArg(), new ParseField("_id"));
        HIT_PARSER.declareLong(optionalConstructorArg(), new ParseField("_version"));
        HIT_PARSER.declareObject(((basicHit, tuple) -> basicHit.setSource(tuple.v1(), tuple.v2())), (p, s) -> {
            try {
                /*
                 * We spool the data from the remote back into xcontent so we can get bytes to send. There ought to be a better way but for
                 * now this should do.
                 */
                try (XContentBuilder b = XContentBuilder.builder(s.xContent())) {
                    b.copyCurrentStructure(p);
                    // a hack but this lets us get the right xcontent type to go with the source
                    return new Tuple<>(BytesReference.bytes(b), s);
                }
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "[hit] failed to parse [_source]", e);
            }
        }, new ParseField("_source"));
        ParseField routingField = new ParseField("_routing");
        ParseField ttlField = new ParseField("_ttl");
        ParseField parentField = new ParseField("_parent");
        HIT_PARSER.declareString(BasicHit::setRouting, routingField);
        // Pre-2.0.0 routing come back in "fields"
        class Fields {
            String routing;
        }
        ObjectParser<Fields, MediaType> fieldsParser = new ObjectParser<>("fields", Fields::new);
        HIT_PARSER.declareObject((hit, fields) -> { hit.setRouting(fields.routing); }, fieldsParser, new ParseField("fields"));
        fieldsParser.declareString((fields, routing) -> fields.routing = routing, routingField);
        fieldsParser.declareLong((fields, ttl) -> {}, ttlField); // ignore ttls since they have been removed
        fieldsParser.declareString((fields, parent) -> {}, parentField); // ignore parents since they have been removed
    }

    /**
     * Parser for the {@code hits} element. Parsed to an array of {@code [total (Long), hits (List<Hit>)]}.
     */
    public static final ConstructingObjectParser<Object[], MediaType> HITS_PARSER = new ConstructingObjectParser<>("hits", true, a -> a);
    static {
        HITS_PARSER.declareField(constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                final TotalHits totalHits = SearchHits.parseTotalHitsFragment(p);
                assert totalHits.relation() == TotalHits.Relation.EQUAL_TO;
                return totalHits.value();
            } else {
                // For BWC with nodes pre 7.0
                return p.longValue();
            }
        }, new ParseField("total"), ValueType.OBJECT_OR_LONG);
        HITS_PARSER.declareObjectArray(constructorArg(), HIT_PARSER, new ParseField("hits"));
    }

    /**
     * Parser for {@code failed} shards in the {@code _shards} elements.
     */
    public static final ConstructingObjectParser<SearchFailure, Void> SEARCH_FAILURE_PARSER = new ConstructingObjectParser<>(
        "failure",
        true,
        a -> {
            int i = 0;
            String index = (String) a[i++];
            Integer shardId = (Integer) a[i++];
            String nodeId = (String) a[i++];
            Object reason = a[i++];

            Throwable reasonThrowable;
            if (reason instanceof String) {
                reasonThrowable = new RuntimeException("Unknown remote exception with reason=[" + (String) reason + "]");
            } else {
                reasonThrowable = (Throwable) reason;
            }
            return new SearchFailure(reasonThrowable, index, shardId, nodeId);
        }
    );
    static {
        SEARCH_FAILURE_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("index"));
        SEARCH_FAILURE_PARSER.declareInt(optionalConstructorArg(), new ParseField("shard"));
        SEARCH_FAILURE_PARSER.declareString(optionalConstructorArg(), new ParseField("node"));
        SEARCH_FAILURE_PARSER.declareField(constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return ThrowableBuilder.PARSER.apply(p, null);
            } else {
                return p.text();
            }
        }, new ParseField("reason"), ValueType.OBJECT_OR_STRING);
    }

    /**
     * Parser for the {@code _shards} element. Throws everything out except the errors array if there is one. If there isn't one then it
     * parses to an empty list.
     */
    public static final ConstructingObjectParser<List<Throwable>, Void> SHARDS_PARSER = new ConstructingObjectParser<>(
        "_shards",
        true,
        a -> {
            @SuppressWarnings("unchecked")
            List<Throwable> failures = (List<Throwable>) a[0];
            failures = failures == null ? emptyList() : failures;
            return failures;
        }
    );
    static {
        SHARDS_PARSER.declareObjectArray(optionalConstructorArg(), SEARCH_FAILURE_PARSER, new ParseField("failures"));
    }

    public static final ConstructingObjectParser<Response, MediaType> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "search_response",
        true,
        a -> {
            int i = 0;
            Throwable catastrophicFailure = (Throwable) a[i++];
            if (catastrophicFailure != null) {
                return new Response(false, singletonList(new SearchFailure(catastrophicFailure)), 0, emptyList(), null);
            }
            boolean timedOut = (boolean) a[i++];
            String scroll = (String) a[i++];
            Object[] hitsElement = (Object[]) a[i++];
            @SuppressWarnings("unchecked")
            List<SearchFailure> failures = (List<SearchFailure>) a[i++];

            long totalHits = 0;
            List<Hit> hits = emptyList();

            // Pull apart the hits element if we got it
            if (hitsElement != null) {
                i = 0;
                totalHits = (long) hitsElement[i++];
                @SuppressWarnings("unchecked")
                List<Hit> h = (List<Hit>) hitsElement[i++];
                hits = h;
            }

            return new Response(timedOut, failures, totalHits, hits, scroll);
        }
    );
    static {
        RESPONSE_PARSER.declareObject(optionalConstructorArg(), (p, c) -> ThrowableBuilder.PARSER.apply(p, null), new ParseField("error"));
        RESPONSE_PARSER.declareBoolean(optionalConstructorArg(), new ParseField("timed_out"));
        RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField("_scroll_id"));
        RESPONSE_PARSER.declareObject(optionalConstructorArg(), HITS_PARSER, new ParseField("hits"));
        RESPONSE_PARSER.declareObject(optionalConstructorArg(), (p, c) -> SHARDS_PARSER.apply(p, null), new ParseField("_shards"));
    }

    /**
     * Collects stuff about Throwables and attempts to rebuild them.
     */
    public static class ThrowableBuilder {
        public static final BiFunction<XContentParser, Void, Throwable> PARSER;
        static {
            ObjectParser<ThrowableBuilder, Void> parser = new ObjectParser<>("reason", true, ThrowableBuilder::new);
            PARSER = parser.andThen(ThrowableBuilder::build);
            parser.declareString(ThrowableBuilder::setType, new ParseField("type"));
            parser.declareString(ThrowableBuilder::setReason, new ParseField("reason"));
            parser.declareObject(ThrowableBuilder::setCausedBy, PARSER::apply, new ParseField("caused_by"));

            // So we can give a nice error for parsing exceptions
            parser.declareInt(ThrowableBuilder::setLine, new ParseField("line"));
            parser.declareInt(ThrowableBuilder::setColumn, new ParseField("col"));
        }

        private String type;
        private String reason;
        private Integer line;
        private Integer column;
        private Throwable causedBy;

        public Throwable build() {
            Throwable t = buildWithoutCause();
            if (causedBy != null) {
                t.initCause(causedBy);
            }
            return t;
        }

        private Throwable buildWithoutCause() {
            requireNonNull(type, "[type] is required");
            requireNonNull(reason, "[reason] is required");
            switch (type) {
                // Make some effort to use the right exceptions
                case "rejected_execution_exception":
                    return new OpenSearchRejectedExecutionException(reason);
                case "parsing_exception":
                    XContentLocation location = null;
                    if (line != null && column != null) {
                        location = new XContentLocation(line, column);
                    }
                    return new ParsingException(location, reason);
                // But it isn't worth trying to get it perfect....
                default:
                    return new RuntimeException(type + ": " + reason);
            }
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        public void setLine(Integer line) {
            this.line = line;
        }

        public void setColumn(Integer column) {
            this.column = column;
        }

        public void setCausedBy(Throwable causedBy) {
            this.causedBy = causedBy;
        }
    }

    /**
     * Parses the main action to return just the {@linkplain Version} that it returns. We throw everything else out.
     */
    public static final ConstructingObjectParser<Version, MediaType> MAIN_ACTION_PARSER = new ConstructingObjectParser<>(
        "/",
        true,
        a -> (Version) a[0]
    );
    static {
        ConstructingObjectParser<Version, MediaType> versionParser = new ConstructingObjectParser<>(
            "version",
            true,
            a -> a[0] == null
                ? LegacyESVersion.fromString(((String) a[1]).replace("-SNAPSHOT", "").replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", ""))
                : Version.fromString(((String) a[1]).replace("-SNAPSHOT", "").replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", ""))
        );
        versionParser.declareStringOrNull(optionalConstructorArg(), new ParseField("distribution"));
        versionParser.declareString(constructorArg(), new ParseField("number"));
        MAIN_ACTION_PARSER.declareObject(constructorArg(), versionParser, new ParseField("version"));
    }
}
