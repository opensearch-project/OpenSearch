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

package org.opensearch.cluster;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Abstract diffable object with simple diffs implementation that sends the entire object if object has changed or
 * nothing if object remained the same.
 *
 * @opensearch.internal
 */
public abstract class AbstractDiffable<T extends Diffable<T>> implements Diffable<T> {

    private static final Diff<?> EMPTY = new CompleteDiff<>();

    @SuppressWarnings("unchecked")
    @Override
    public Diff<T> diff(T previousState) {
        if (this.equals(previousState)) {
            return (Diff<T>) EMPTY;
        } else {
            return new CompleteDiff<>((T) this);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Diffable<T>> Diff<T> readDiffFrom(Reader<T> reader, StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new CompleteDiff<>(reader.read(in));
        }
        return (Diff<T>) EMPTY;
    }

    /**
     * A complete diff.
     *
     * @opensearch.internal
     */
    private static class CompleteDiff<T extends Diffable<T>> implements Diff<T> {

        @Nullable
        private final T part;

        /**
         * Creates simple diff with changes
         */
        CompleteDiff(T part) {
            this.part = part;
        }

        @Override
        public String toString() {
            return "CompleteDiff{" + "part=" + part + '}';
        }

        /**
         * Creates simple diff without changes
         */
        CompleteDiff() {
            this.part = null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (part != null) {
                out.writeBoolean(true);
                part.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public T apply(T part) {
            if (this.part != null) {
                return this.part;
            } else {
                return part;
            }
        }
    }
}
