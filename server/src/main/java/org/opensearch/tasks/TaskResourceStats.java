/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.tasks.Task.THREAD_INFO;

/**
 * Resource information about a currently running task.
 * <p>
 * Writeable TaskResourceStats objects are used to represent resource
 * snapshot information about currently running task.
 *
 * @opensearch.internal
 */
public class TaskResourceStats implements Writeable, ToXContentFragment {
    private final Map<String, TaskResourceUsage> resourceUsage;
    private final TaskThreadUsage threadUsage;

    public TaskResourceStats(Map<String, TaskResourceUsage> resourceUsage, TaskThreadUsage threadUsage) {
        this.resourceUsage = Objects.requireNonNull(resourceUsage, "resource usage is required");
        this.threadUsage = Objects.requireNonNull(threadUsage, "thread usage is required");
    }

    /**
     * Read from a stream.
     */
    public TaskResourceStats(StreamInput in) throws IOException {
        resourceUsage = in.readMap(StreamInput::readString, TaskResourceUsage::readFromStream);
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            threadUsage = TaskThreadUsage.readFromStream(in);
        } else {
            // Initialize TaskThreadUsage in case it is not found in mixed cluster case
            threadUsage = new TaskThreadUsage(0, 0);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(resourceUsage, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            threadUsage.writeTo(out);
        }
    }

    public Map<String, TaskResourceUsage> getResourceUsageInfo() {
        return resourceUsage;
    }

    public TaskThreadUsage getThreadUsage() {
        return threadUsage;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, TaskResourceUsage> resourceUsageEntry : resourceUsage.entrySet()) {
            builder.startObject(resourceUsageEntry.getKey());
            if (resourceUsageEntry.getValue() != null) {
                resourceUsageEntry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.startObject(THREAD_INFO);
        threadUsage.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static TaskResourceStats fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        final Map<String, TaskResourceUsage> resourceStats = new HashMap<>();
        // Initialize TaskThreadUsage in case it is not found in mixed cluster case
        TaskThreadUsage threadUsage = new TaskThreadUsage(0, 0);
        if (token == XContentParser.Token.FIELD_NAME) {
            assert parser.currentToken() == XContentParser.Token.FIELD_NAME : "Expected field name but saw [" + parser.currentToken() + "]";
            do {
                // Must point to field name
                String fieldName = parser.currentName();
                // And then the value

                if (fieldName.equals(THREAD_INFO)) {
                    threadUsage = TaskThreadUsage.fromXContent(parser);
                } else {
                    TaskResourceUsage value = TaskResourceUsage.fromXContent(parser);
                    resourceStats.put(fieldName, value);
                }
            } while (parser.nextToken() == XContentParser.Token.FIELD_NAME);
        }
        return new TaskResourceStats(resourceStats, threadUsage);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResourceStats.class) {
            return false;
        }
        TaskResourceStats other = (TaskResourceStats) obj;
        return Objects.equals(resourceUsage, other.resourceUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceUsage);
    }
}
