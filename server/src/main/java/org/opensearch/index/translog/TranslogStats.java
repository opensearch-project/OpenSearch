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

package org.opensearch.index.translog;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Translog statistics
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TranslogStats implements Writeable, ToXContentFragment {
    private static final String TRANSLOG = "translog";
    private long translogSizeInBytes;
    private int numberOfOperations;
    private long uncommittedSizeInBytes;
    private int uncommittedOperations;
    private long earliestLastModifiedAge;

    /**
     * Stats related to the Remote Translog Store operations
     */
    private final RemoteTranslogStats remoteTranslogStats;

    public TranslogStats() {
        remoteTranslogStats = new RemoteTranslogStats();
    }

    public TranslogStats(StreamInput in) throws IOException {
        numberOfOperations = in.readVInt();
        translogSizeInBytes = in.readVLong();
        uncommittedOperations = in.readVInt();
        uncommittedSizeInBytes = in.readVLong();
        earliestLastModifiedAge = in.readVLong();
        remoteTranslogStats = in.getVersion().onOrAfter(Version.V_2_10_0)
            ? in.readOptionalWriteable(RemoteTranslogStats::new)
            : new RemoteTranslogStats();
    }

    public TranslogStats(
        int numberOfOperations,
        long translogSizeInBytes,
        int uncommittedOperations,
        long uncommittedSizeInBytes,
        long earliestLastModifiedAge
    ) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        if (uncommittedOperations < 0) {
            throw new IllegalArgumentException("uncommittedOperations must be >= 0");
        }
        if (uncommittedSizeInBytes < 0) {
            throw new IllegalArgumentException("uncommittedSizeInBytes must be >= 0");
        }
        if (earliestLastModifiedAge < 0) {
            throw new IllegalArgumentException("earliestLastModifiedAge must be >= 0");
        }

        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
        this.uncommittedSizeInBytes = uncommittedSizeInBytes;
        this.uncommittedOperations = uncommittedOperations;
        this.earliestLastModifiedAge = earliestLastModifiedAge;
        this.remoteTranslogStats = new RemoteTranslogStats();
    }

    public void addRemoteTranslogStats(RemoteTranslogStats remoteTranslogStats) {
        if (this.remoteTranslogStats != null) {
            this.remoteTranslogStats.add(remoteTranslogStats);
        }
    }

    public void add(TranslogStats other) {
        if (other == null) {
            return;
        }

        this.numberOfOperations += other.numberOfOperations;
        this.translogSizeInBytes += other.translogSizeInBytes;
        this.uncommittedOperations += other.uncommittedOperations;
        this.uncommittedSizeInBytes += other.uncommittedSizeInBytes;
        if (this.earliestLastModifiedAge == 0) {
            this.earliestLastModifiedAge = other.earliestLastModifiedAge;
        } else {
            this.earliestLastModifiedAge = Math.min(this.earliestLastModifiedAge, other.earliestLastModifiedAge);
        }

        addRemoteTranslogStats(other.remoteTranslogStats);
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public int estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    /** the size of the generations in the translog that weren't yet to committed to lucene */
    public long getUncommittedSizeInBytes() {
        return uncommittedSizeInBytes;
    }

    /** the number of operations in generations of the translog that weren't yet to committed to lucene */
    public int getUncommittedOperations() {
        return uncommittedOperations;
    }

    public long getEarliestLastModifiedAge() {
        return earliestLastModifiedAge;
    }

    public RemoteTranslogStats getRemoteTranslogStats() {
        return remoteTranslogStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TRANSLOG);
        addLocalTranslogStatsXContent(builder);
        if (remoteTranslogStats != null) {
            builder = remoteTranslogStats.toXContent(builder, params);
        }

        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfOperations);
        out.writeVLong(translogSizeInBytes);
        out.writeVInt(uncommittedOperations);
        out.writeVLong(uncommittedSizeInBytes);
        out.writeVLong(earliestLastModifiedAge);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalWriteable(remoteTranslogStats);
        }
    }

    private void addLocalTranslogStatsXContent(XContentBuilder builder) throws IOException {
        builder.field("operations", numberOfOperations);
        builder.humanReadableField("size_in_bytes", "size", new ByteSizeValue(translogSizeInBytes));
        builder.field("uncommitted_operations", uncommittedOperations);
        builder.humanReadableField("uncommitted_size_in_bytes", "uncommitted_size", new ByteSizeValue(uncommittedSizeInBytes));
        builder.field("earliest_last_modified_age", earliestLastModifiedAge);
    }
}
