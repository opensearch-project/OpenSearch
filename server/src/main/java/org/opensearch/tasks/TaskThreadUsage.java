/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Task thread execution information.
 * Writeable TaskThreadExecutions objects are used to represent thread related resource usage of running tasks.
 * asd
 *
 * @opensearch.internal
 */
public class TaskThreadUsage implements Writeable, ToXContentFragment {

    private static final String THREAD_EXECUTIONS = "thread_executions";
    private static final String ACTIVE_THREADS = "active_threads";
    private static final ParseField THREAD_EXECUTION_COUNT = new ParseField(THREAD_EXECUTIONS);
    private static final ParseField ACTIVE_THREAD_COUNT = new ParseField(ACTIVE_THREADS);

    private final int threadExecutions;
    private final int activeThreads;

    public TaskThreadUsage(int threadExecutions, int activeThreads) {
        this.threadExecutions = threadExecutions;
        this.activeThreads = activeThreads;
    }

    /**
     * Read from a stream.
     */
    public static TaskThreadUsage readFromStream(StreamInput in) throws IOException {
        return new TaskThreadUsage(in.readInt(), in.readInt());
    }

    public int getThreadExecutions() {
        return threadExecutions;
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(THREAD_EXECUTIONS, threadExecutions);
        builder.field(ACTIVE_THREADS, activeThreads);
        return builder;
    }

    public static final ConstructingObjectParser<TaskThreadUsage, Void> PARSER = new ConstructingObjectParser<>(
        "task_thread_executions",
        a -> new TaskThreadUsage((int) a[0], (int) a[1])
    );

    static {
        PARSER.declareInt(constructorArg(), THREAD_EXECUTION_COUNT);
        PARSER.declareInt(constructorArg(), ACTIVE_THREAD_COUNT);
    }

    public static TaskThreadUsage fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(threadExecutions);
        out.writeInt(activeThreads);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskThreadUsage.class) {
            return false;
        }
        TaskThreadUsage other = (TaskThreadUsage) obj;
        return Objects.equals(threadExecutions, other.threadExecutions) && Objects.equals(activeThreads, other.activeThreads);
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadExecutions, activeThreads);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }
}
