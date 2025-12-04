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

package org.opensearch.script;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stats for scripts
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ScriptStats implements Writeable, ToXContentFragment {
    private final List<ScriptContextStats> contextStats;
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new ScriptStats object.
     * @param builder The builder instance containing all the values.
     */
    private ScriptStats(Builder builder) {
        this.contextStats = builder.contextStats;
        this.compilations = builder.compilations;
        this.cacheEvictions = builder.cacheEvictions;
        this.compilationLimitTriggered = builder.compilationLimitTriggered;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link ScriptStats#aggregate(List)} instead.
     */
    @Deprecated
    public ScriptStats(List<ScriptContextStats> contextStats) {
        ArrayList<ScriptContextStats> ctxStats = new ArrayList<>(contextStats.size());
        ctxStats.addAll(contextStats);
        ctxStats.sort(ScriptContextStats::compareTo);
        this.contextStats = Collections.unmodifiableList(ctxStats);
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptContextStats stats : contextStats) {
            compilations += stats.getCompilations();
            cacheEvictions += stats.getCacheEvictions();
            compilationLimitTriggered += stats.getCompilationLimitTriggered();
        }
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public ScriptStats(long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.contextStats = Collections.emptyList();
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public ScriptStats(ScriptContextStats context) {
        this(context.getCompilations(), context.getCacheEvictions(), context.getCompilationLimitTriggered());
    }

    public ScriptStats(StreamInput in) throws IOException {
        compilations = in.readVLong();
        cacheEvictions = in.readVLong();
        compilationLimitTriggered = in.readVLong();
        contextStats = in.readList(ScriptContextStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(compilations);
        out.writeVLong(cacheEvictions);
        out.writeVLong(compilationLimitTriggered);
        out.writeList(contextStats);
    }

    /**
     * Aggregates a list of {@link ScriptContextStats} into a {@link ScriptStats}.
     * Sums all metrics across contexts and returns an immutable, sorted, and aggregated result.
     */
    public static ScriptStats aggregate(List<ScriptContextStats> contextStats) {
        ArrayList<ScriptContextStats> ctxStats = new ArrayList<>(contextStats);
        ctxStats.sort(ScriptContextStats::compareTo);

        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptContextStats stat : ctxStats) {
            compilations += stat.getCompilations();
            cacheEvictions += stat.getCacheEvictions();
            compilationLimitTriggered += stat.getCompilationLimitTriggered();
        }

        return new Builder().contextStats(Collections.unmodifiableList(new ArrayList<>(ctxStats)))
            .compilations(compilations)
            .cacheEvictions(cacheEvictions)
            .compilationLimitTriggered(compilationLimitTriggered)
            .build();
    }

    public List<ScriptContextStats> getContextStats() {
        return contextStats;
    }

    public long getCompilations() {
        return compilations;
    }

    public long getCacheEvictions() {
        return cacheEvictions;
    }

    public long getCompilationLimitTriggered() {
        return compilationLimitTriggered;
    }

    public ScriptCacheStats toScriptCacheStats() {
        if (contextStats.isEmpty()) {
            return new ScriptCacheStats(this);
        }

        Map<String, ScriptStats> contexts = new HashMap<>(contextStats.size());
        for (ScriptContextStats contextStats : contextStats) {
            contexts.put(
                contextStats.getContext(),
                new ScriptStats.Builder().compilations(contextStats.getCompilations())
                    .cacheEvictions(contextStats.getCacheEvictions())
                    .compilationLimitTriggered(contextStats.getCompilationLimitTriggered())
                    .build()
            );
        }
        return new ScriptCacheStats(contexts);
    }

    /**
     * Builder for the {@link ScriptStats} class.
     * Provides a fluent API for constructing a ScriptStats object.
     */
    public static class Builder {
        private List<ScriptContextStats> contextStats = Collections.emptyList();
        private long compilations = 0;
        private long cacheEvictions = 0;
        private long compilationLimitTriggered = 0;

        public Builder() {}

        public Builder contextStats(List<ScriptContextStats> contextStats) {
            this.contextStats = contextStats;
            return this;
        }

        public Builder compilations(long compilations) {
            this.compilations = compilations;
            return this;
        }

        public Builder cacheEvictions(long cacheEvictions) {
            this.cacheEvictions = cacheEvictions;
            return this;
        }

        public Builder compilationLimitTriggered(long compilationLimitTriggered) {
            this.compilationLimitTriggered = compilationLimitTriggered;
            return this;
        }

        /**
         * Creates a {@link ScriptStats} object from the builder's current state.
         * @return A new ScriptStats instance.
         */
        public ScriptStats build() {
            return new ScriptStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(Fields.COMPILATIONS, compilations);
        builder.field(Fields.CACHE_EVICTIONS, cacheEvictions);
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String CONTEXTS = "contexts";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }
}
