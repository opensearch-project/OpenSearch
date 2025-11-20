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

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;

import java.util.Objects;

/**
 * Base class for rollover request conditions
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class Condition<T> implements NamedWriteable, ToXContentFragment {

    protected T value;
    protected final String name;

    protected Condition(String name) {
        this.name = name;
    }

    public abstract Result evaluate(Stats stats);

    /**
     * Checks if this condition is available in a specific version.
     * This makes sure BWC when introducing a new condition which is not recognized by older versions.
     */
    boolean includedInVersion(Version version) {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Condition<?> condition = (Condition<?>) o;
        return Objects.equals(value, condition.value) && Objects.equals(name, condition.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, name);
    }

    @Override
    public final String toString() {
        return "[" + name + ": " + value + "]";
    }

    public T value() {
        return value;
    }

    public String name() {
        return name;
    }

    /**
     * Holder for index stats used to evaluate conditions
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Stats {
        public final long numDocs;
        public final long indexCreated;
        public final ByteSizeValue indexSize;

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder) {
            this.numDocs = builder.numDocs;
            this.indexCreated = builder.indexCreated;
            this.indexSize = builder.indexSize;
        }

        /**
         * This constructor will be deprecated starting in version 3.4.0.
         * Use {@link Builder} instead.
         */
        @Deprecated
        public Stats(long numDocs, long indexCreated, ByteSizeValue indexSize) {
            this.numDocs = numDocs;
            this.indexCreated = indexCreated;
            this.indexSize = indexSize;
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private long numDocs = 0;
            private long indexCreated = 0;
            private ByteSizeValue indexSize = null;

            public Builder() {}

            public Builder numDocs(long numDocs) {
                this.numDocs = numDocs;
                return this;
            }

            public Builder indexCreated(long indexCreated) {
                this.indexCreated = indexCreated;
                return this;
            }

            public Builder indexSize(ByteSizeValue size) {
                this.indexSize = size;
                return this;
            }

            /**
             * Creates a {@link Stats} object from the builder's current state.
             * @return A new Stats instance.
             */
            public Stats build() {
                return new Stats(this);
            }
        }
    }

    /**
     * Holder for evaluated condition result
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Result {
        public final Condition<?> condition;
        public final boolean matched;

        protected Result(Condition<?> condition, boolean matched) {
            this.condition = condition;
            this.matched = matched;
        }
    }
}
