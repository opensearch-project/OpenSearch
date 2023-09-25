/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stores stats about unreferenced file cleanup due to segment merge failure.
 *
 * @opensearch.internal
 */
public class UnreferencedFileCleanUpStats implements Writeable, ToXContentFragment {

    private long totalUnreferencedFileCleanupCount;

    public UnreferencedFileCleanUpStats() {

    }

    public UnreferencedFileCleanUpStats(StreamInput in) throws IOException {
        totalUnreferencedFileCleanupCount = in.readVLong();
    }

    public void add(long totalUnreferencedFileCleanupCount) {
        this.totalUnreferencedFileCleanupCount += totalUnreferencedFileCleanupCount;
    }

    public void add(UnreferencedFileCleanUpStats cleanUpStats) {
        if (cleanUpStats == null) {
            return;
        }

        this.totalUnreferencedFileCleanupCount += cleanUpStats.totalUnreferencedFileCleanupCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.UNREFERENCED_FILE_CLEANUP);
        builder.field(Fields.COUNT, totalUnreferencedFileCleanupCount);
        builder.endObject();
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalUnreferencedFileCleanupCount);
    }

    /**
     * Fields for unreferenced file cleanup statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String UNREFERENCED_FILE_CLEANUP = "unreferenced_file_cleanup";
        static final String COUNT = "count";
    }

}
