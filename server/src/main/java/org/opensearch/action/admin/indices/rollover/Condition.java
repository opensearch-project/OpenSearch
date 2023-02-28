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
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;

import java.util.Objects;

/**
 * Base class for rollover request conditions
 *
 * @opensearch.internal
 */
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
     * @opensearch.internal
     */
    public static class Stats {
        public final long numDocs;
        public final long indexCreated;
        public final ByteSizeValue indexSize;

        public Stats(long numDocs, long indexCreated, ByteSizeValue indexSize) {
            this.numDocs = numDocs;
            this.indexCreated = indexCreated;
            this.indexSize = indexSize;
        }
    }

    /**
     * Holder for evaluated condition result
     *
     * @opensearch.internal
     */
    public static class Result {
        public final Condition<?> condition;
        public final boolean matched;

        protected Result(Condition<?> condition, boolean matched) {
            this.condition = condition;
            this.matched = matched;
        }
    }
}
