/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.threadpool;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
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
* @opensearch.internal
*/
public class ProtobufThreadPoolStats implements ProtobufWriteable, ToXContentFragment, Iterable<ProtobufThreadPoolStats.Stats> {

    /**
     * The statistics.
    *
    * @opensearch.internal
    */
    public static class Stats implements ProtobufWriteable, ToXContentFragment, Comparable<Stats> {

        private final String name;
        private final int threads;
        private final int queue;
        private final int active;
        private final long rejected;
        private final int largest;
        private final long completed;

        public Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
        }

        public Stats(CodedInputStream in) throws IOException {
            name = in.readString();
            threads = in.readInt32();
            queue = in.readInt32();
            active = in.readInt32();
            rejected = in.readInt64();
            largest = in.readInt32();
            completed = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeStringNoTag(name);
            out.writeInt32NoTag(threads);
            out.writeInt32NoTag(queue);
            out.writeInt32NoTag(active);
            out.writeInt64NoTag(rejected);
            out.writeInt32NoTag(largest);
            out.writeInt64NoTag(completed);
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
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
    }

    private List<Stats> stats;

    public ProtobufThreadPoolStats(List<Stats> stats) {
        Collections.sort(stats);
        this.stats = stats;
    }

    public ProtobufThreadPoolStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        stats = protobufStreamInput.readList(Stats::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeCollection(stats, (o, v) -> v.writeTo(o));
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
