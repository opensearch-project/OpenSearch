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

package org.opensearch.threadpool;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Stats for a threadpool
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ThreadPoolStats implements Writeable, ToXContentFragment, Iterable<ThreadPoolStats.Stats> {

    /**
     * The statistics.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Stats implements Writeable, ToXContentFragment, Comparable<Stats> {

        private final String name;
        private final int threads;
        private final int queue;
        private final int active;
        private final long rejected;
        private final int largest;
        private final long completed;
        private final long waitTimeNanos;
        private final int parallelism;

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder) {
            this.name = builder.name;
            this.threads = builder.threads;
            this.queue = builder.queue;
            this.active = builder.active;
            this.rejected = builder.rejected;
            this.largest = builder.largest;
            this.completed = builder.completed;
            this.waitTimeNanos = builder.waitTimeNanos;
            this.parallelism = builder.parallelism;
        }

        /**
         * This constructor will be deprecated starting in version 3.3.0.
         * Use {@link Builder} instead.
         */
        @Deprecated
        public Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed, long waitTimeNanos) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
            this.waitTimeNanos = waitTimeNanos;
            this.parallelism = -1;
        }

        public Stats(
            String name,
            int threads,
            int queue,
            int active,
            long rejected,
            int largest,
            long completed,
            long waitTimeNanos,
            int parallelism
        ) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
            this.waitTimeNanos = waitTimeNanos;
            this.parallelism = parallelism;
        }

        public Stats(StreamInput in) throws IOException {
            name = in.readString();
            threads = in.readInt();
            queue = in.readInt();
            active = in.readInt();
            rejected = in.readLong();
            largest = in.readInt();
            completed = in.readLong();
            waitTimeNanos = in.getVersion().onOrAfter(Version.V_2_11_0) ? in.readLong() : -1;
            parallelism = in.getVersion().onOrAfter(Version.V_3_4_0) ? in.readInt() : -1;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(threads);
            out.writeInt(queue);
            out.writeInt(active);
            out.writeLong(rejected);
            out.writeInt(largest);
            out.writeLong(completed);
            if (out.getVersion().onOrAfter(Version.V_2_11_0)) {
                out.writeLong(waitTimeNanos);
            }
            if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
                out.writeInt(parallelism);
            }
        }

        public String getName() {
            return this.name;
        }

        public int getThreads() {
            return this.threads;
        }

        public int getQueue() {
            return this.queue;
        }

        public int getActive() {
            return this.active;
        }

        public long getRejected() {
            return rejected;
        }

        public int getLargest() {
            return largest;
        }

        public long getCompleted() {
            return this.completed;
        }

        public TimeValue getWaitTime() {
            return TimeValue.timeValueNanos(waitTimeNanos);
        }

        public long getWaitTimeNanos() {
            return waitTimeNanos;
        }

        public int getParallelism() {
            return parallelism;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            if (parallelism != -1) {
                builder.field(Fields.PARALLELISM, parallelism);
            }
            if (threads != -1) {
                builder.field(Fields.THREADS, threads);
            }
            if (queue != -1) {
                builder.field(Fields.QUEUE, queue);
            }
            if (active != -1) {
                builder.field(Fields.ACTIVE, active);
            }
            if (rejected != -1) {
                builder.field(Fields.REJECTED, rejected);
            }
            if (largest != -1) {
                builder.field(Fields.LARGEST, largest);
            }
            if (completed != -1) {
                builder.field(Fields.COMPLETED, completed);
            }
            if (waitTimeNanos != -1) {
                if (builder.humanReadable()) {
                    builder.field(Fields.WAIT_TIME, getWaitTime());
                }
                builder.field(Fields.WAIT_TIME_NANOS, getWaitTimeNanos());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int compareTo(Stats other) {
            if ((getName() == null) && (other.getName() == null)) {
                return 0;
            } else if ((getName() != null) && (other.getName() == null)) {
                return 1;
            } else if (getName() == null) {
                return -1;
            } else {
                int compare = getName().compareTo(other.getName());
                if (compare == 0) {
                    compare = Integer.compare(getThreads(), other.getThreads());
                }
                return compare;
            }
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private String name = "";
            private int threads = 0;
            private int queue = 0;
            private int active = 0;
            private long rejected = 0;
            private int largest = 0;
            private long completed = 0;
            private long waitTimeNanos = 0;
            private int parallelism = 0;

            public Builder() {}

            public Builder name(String name) {
                this.name = name;
                return this;
            }

            public Builder threads(int threads) {
                this.threads = threads;
                return this;
            }

            public Builder queue(int queue) {
                this.queue = queue;
                return this;
            }

            public Builder active(int active) {
                this.active = active;
                return this;
            }

            public Builder rejected(long rejected) {
                this.rejected = rejected;
                return this;
            }

            public Builder largest(int largest) {
                this.largest = largest;
                return this;
            }

            public Builder completed(long completed) {
                this.completed = completed;
                return this;
            }

            public Builder waitTimeNanos(long waitTimeNanos) {
                this.waitTimeNanos = waitTimeNanos;
                return this;
            }

            public Builder parallelism(int parallelism) {
                this.parallelism = parallelism;
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

    private List<Stats> stats;

    public ThreadPoolStats(List<Stats> stats) {
        Collections.sort(stats);
        this.stats = stats;
    }

    public ThreadPoolStats(StreamInput in) throws IOException {
        stats = in.readList(Stats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(stats);
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    static final class Fields {
        static final String THREAD_POOL = "thread_pool";
        static final String THREADS = "threads";
        static final String QUEUE = "queue";
        static final String ACTIVE = "active";
        static final String REJECTED = "rejected";
        static final String LARGEST = "largest";
        static final String COMPLETED = "completed";
        static final String WAIT_TIME = "total_wait_time";
        static final String WAIT_TIME_NANOS = "total_wait_time_in_nanos";
        static final String PARALLELISM = "parallelism";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (Stats stat : stats) {
            stat.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
