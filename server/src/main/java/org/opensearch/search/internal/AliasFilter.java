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

package org.opensearch.search.internal;

import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.Rewriteable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a {@link QueryBuilder} and a list of alias names that filters the builder is composed of.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class AliasFilter implements Writeable, Rewriteable<AliasFilter> {

    /**
     * Where the alias filter is applied in the query pipeline.
     * <p>
     * {@link #POST_FILTER} is the historical behavior: the filter is added as a
     * post-collection filter, so statistics and scoring still run over every
     * document in the shard. This is the mode any pre-3.8 node speaks over the
     * wire, and it is the default for every code path today.
     * <p>
     * {@link #PRE_FILTER} applies the alias filter before scoring / term
     * enumeration, so BM25 collection statistics reflect only the documents the
     * alias admits rather than the whole shard. See the design notes on
     * filter-aware aliases.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum Enforcement implements Writeable {
        POST_FILTER("post_filter"),
        PRE_FILTER("pre_filter");

        private final String value;

        Enforcement(String value) {
            this.value = value;
        }

        /** The canonical REST/XContent string for this enforcement mode (e.g. {@code "pre_filter"}). */
        public String value() {
            return value;
        }

        /**
         * Parse an enforcement mode from its REST/XContent string. Returns {@link #POST_FILTER} for a
         * {@code null} value so an unspecified enforcement keeps today's behavior.
         *
         * @throws IllegalArgumentException if {@code value} is non-null but not a known mode
         */
        public static Enforcement fromString(String value) {
            if (value == null) {
                return POST_FILTER;
            }
            for (Enforcement e : values()) {
                if (e.value.equals(value)) {
                    return e;
                }
            }
            throw new IllegalArgumentException("unknown alias enforcement [" + value + "], expected one of [post_filter, pre_filter]");
        }

        public static Enforcement readFrom(StreamInput in) throws IOException {
            return in.readEnum(Enforcement.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }

    /**
     * First OpenSearch version that carries the {@link Enforcement} field on the wire.
     * <p>
     * TODO(filter-aware-alias): pinned to V_3_8_0 for now because main is still on 3.8 and V_3_9_0 does
     * not yet exist; bump to the release this actually ships in once main is bumped. The same gate is
     * duplicated for this field's other wire representations in IndicesAliasesRequest.AliasActions and
     * AliasMetadata (grep "TODO(filter-aware-alias)") -- update all three together.
     */
    static final Version ENFORCEMENT_VERSION = Version.V_3_8_0;

    private final String[] aliases;
    private final QueryBuilder filter;
    private final Enforcement enforcement;

    public static final AliasFilter EMPTY = new AliasFilter(null, Strings.EMPTY_ARRAY);

    public AliasFilter(QueryBuilder filter, String... aliases) {
        this(filter, Enforcement.POST_FILTER, aliases);
    }

    public AliasFilter(QueryBuilder filter, Enforcement enforcement, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
        this.enforcement = enforcement == null ? Enforcement.POST_FILTER : enforcement;
    }

    public AliasFilter(StreamInput input) throws IOException {
        aliases = input.readStringArray();
        filter = input.readOptionalNamedWriteable(QueryBuilder.class);
        // BWC: older nodes never write the enforcement field. Default to
        // POST_FILTER (today's behavior) so mixed-version clusters keep working.
        if (input.getVersion().onOrAfter(ENFORCEMENT_VERSION)) {
            enforcement = input.readEnum(Enforcement.class);
        } else {
            enforcement = Enforcement.POST_FILTER;
        }
    }

    @Override
    public AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = this.filter;
        if (queryBuilder != null) {
            QueryBuilder rewrite = Rewriteable.rewrite(queryBuilder, context);
            if (rewrite != queryBuilder) {
                return new AliasFilter(rewrite, enforcement, aliases);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        out.writeOptionalNamedWriteable(filter);
        if (out.getVersion().onOrAfter(ENFORCEMENT_VERSION)) {
            out.writeEnum(enforcement);
        }
        // Pre-3.8 receivers implicitly treat the field as POST_FILTER.
    }

    /**
     * Returns the aliases patters that are used to compose the {@link QueryBuilder}
     * returned from {@link #getQueryBuilder()}
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Returns the alias filter {@link QueryBuilder} or <code>null</code> if there is no such filter
     */
    public QueryBuilder getQueryBuilder() {
        return filter;
    }

    /**
     * Returns where the alias filter should be applied in the query pipeline. Always
     * non-null; defaults to {@link Enforcement#POST_FILTER} for backward compatibility.
     * {@link Enforcement#PRE_FILTER} causes {@code DefaultSearchContext} to apply the filter
     * before scoring so BM25 statistics reflect only the documents the alias admits.
     */
    public Enforcement getEnforcement() {
        return enforcement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasFilter that = (AliasFilter) o;
        return Arrays.equals(aliases, that.aliases) && Objects.equals(filter, that.filter) && enforcement == that.enforcement;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(aliases), filter, enforcement);
    }

    @Override
    public String toString() {
        return "AliasFilter{aliases=" + Arrays.toString(aliases) + ", filter=" + filter + ", enforcement=" + enforcement + '}';
    }
}
