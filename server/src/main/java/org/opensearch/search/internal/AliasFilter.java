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
     * Which corpus statistics a search through the alias scores with. The alias filter applies in every mode.
     * <p>
     * {@link #UNFILTERED_STATS} is the default: ordinary scoring, with statistics over every document in the shard,
     * including documents the alias excludes. A node that predates the field behaves this way.
     * <p>
     * {@link #NO_STATS} scores without corpus statistics: every hit takes a constant score.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum ScoringMode implements Writeable {
        UNFILTERED_STATS("unfiltered_stats"),
        NO_STATS("no_stats");

        private final String value;

        ScoringMode(String value) {
            this.value = value;
        }

        /** The canonical REST/XContent string for this scoring mode (e.g. {@code "no_stats"}). */
        public String value() {
            return value;
        }

        /**
         * Parse a scoring mode from its REST/XContent string. Returns {@link #UNFILTERED_STATS} for a
         * {@code null} value so an unspecified mode keeps today's behavior.
         *
         * @throws IllegalArgumentException if {@code value} is non-null but not a known mode
         */
        public static ScoringMode fromString(String value) {
            if (value == null) {
                return UNFILTERED_STATS;
            }
            for (ScoringMode e : values()) {
                if (e.value.equals(value)) {
                    return e;
                }
            }
            throw new IllegalArgumentException("unknown alias scoring mode [" + value + "], expected one of [unfiltered_stats, no_stats]");
        }

        // By name rather than ordinal, so a mode added later does not need a wire version of its own.
        public static ScoringMode readFrom(StreamInput in) throws IOException {
            return fromString(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

    /**
     * First version that carries {@link ScoringMode} on the wire. The alias request and alias metadata gate their copy
     * of the field on the same version.
     */
    static final Version SCORING_MODE_VERSION = Version.V_3_10_0;

    private final String[] aliases;
    private final QueryBuilder filter;
    private final ScoringMode scoringMode;

    public static final AliasFilter EMPTY = new AliasFilter(null, Strings.EMPTY_ARRAY);

    public AliasFilter(QueryBuilder filter, String... aliases) {
        this(filter, ScoringMode.UNFILTERED_STATS, aliases);
    }

    public AliasFilter(QueryBuilder filter, ScoringMode scoringMode, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
        this.scoringMode = scoringMode == null ? ScoringMode.UNFILTERED_STATS : scoringMode;
    }

    public AliasFilter(StreamInput input) throws IOException {
        aliases = input.readStringArray();
        filter = input.readOptionalNamedWriteable(QueryBuilder.class);
        // BWC: older nodes never write the scoring mode. Default to
        // UNFILTERED_STATS (today's behavior) so mixed-version clusters keep working.
        if (input.getVersion().onOrAfter(SCORING_MODE_VERSION)) {
            scoringMode = ScoringMode.readFrom(input);
        } else {
            scoringMode = ScoringMode.UNFILTERED_STATS;
        }
    }

    @Override
    public AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = this.filter;
        if (queryBuilder != null) {
            QueryBuilder rewrite = Rewriteable.rewrite(queryBuilder, context);
            if (rewrite != queryBuilder) {
                return new AliasFilter(rewrite, scoringMode, aliases);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        out.writeOptionalNamedWriteable(filter);
        if (out.getVersion().onOrAfter(SCORING_MODE_VERSION)) {
            scoringMode.writeTo(out);
        }
        // Pre-3.8 receivers implicitly treat the field as UNFILTERED_STATS.
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
     * Returns the scoring mode for searches through the alias. Never null; defaults to
     * {@link ScoringMode#UNFILTERED_STATS}.
     */
    public ScoringMode getScoringMode() {
        return scoringMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasFilter that = (AliasFilter) o;
        return Arrays.equals(aliases, that.aliases) && Objects.equals(filter, that.filter) && scoringMode == that.scoringMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(aliases), filter, scoringMode);
    }

    @Override
    public String toString() {
        return "AliasFilter{aliases=" + Arrays.toString(aliases) + ", filter=" + filter + ", scoringMode=" + scoringMode + '}';
    }
}
