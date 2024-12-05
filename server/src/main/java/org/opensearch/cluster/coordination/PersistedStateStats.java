/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persisted cluster state related stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.12.0")
public class PersistedStateStats implements Writeable, ToXContentObject {
    private final String statsName;
    private AtomicLong totalTimeInMillis = new AtomicLong(0);
    private AtomicLong failedCount = new AtomicLong(0);
    private AtomicLong successCount = new AtomicLong(0);
    private Map<String, AtomicLong> extendedFields = new HashMap<>(); // keeping minimal extensibility

    public PersistedStateStats(String statsName) {
        this.statsName = statsName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(statsName);
        out.writeVLong(successCount.get());
        out.writeVLong(failedCount.get());
        out.writeVLong(totalTimeInMillis.get());
        if (extendedFields.size() > 0) {
            out.writeBoolean(true);
            out.writeVInt(extendedFields.size());
            for (Map.Entry<String, AtomicLong> extendedField : extendedFields.entrySet()) {
                out.writeString(extendedField.getKey());
                out.writeVLong(extendedField.getValue().get());
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public PersistedStateStats(StreamInput in) throws IOException {
        this.statsName = in.readString();
        this.successCount = new AtomicLong(in.readVLong());
        this.failedCount = new AtomicLong(in.readVLong());
        this.totalTimeInMillis = new AtomicLong(in.readVLong());
        if (in.readBoolean()) {
            int extendedFieldsSize = in.readVInt();
            this.extendedFields = new HashMap<>();
            for (int fieldNumber = 0; fieldNumber < extendedFieldsSize; fieldNumber++) {
                extendedFields.put(in.readString(), new AtomicLong(in.readVLong()));
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(statsName);
        builder.field(Fields.SUCCESS_COUNT, getSuccessCount());
        builder.field(Fields.FAILED_COUNT, getFailedCount());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, getTotalTimeInMillis());
        if (extendedFields.size() > 0) {
            for (Map.Entry<String, AtomicLong> extendedField : extendedFields.entrySet()) {
                builder.field(extendedField.getKey(), extendedField.getValue().get());
            }
        }
        builder.endObject();
        return builder;
    }

    public void stateFailed() {
        failedCount.incrementAndGet();
    }

    public void stateSucceeded() {
        successCount.incrementAndGet();
    }

    /**
     * Expects user to send time taken in milliseconds.
     *
     * @param timeTakenInUpload time taken in uploading the cluster state to remote
     */
    public void stateTook(long timeTakenInUpload) {
        totalTimeInMillis.addAndGet(timeTakenInUpload);
    }

    public long getTotalTimeInMillis() {
        return totalTimeInMillis.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public long getSuccessCount() {
        return successCount.get();
    }

    protected void addToExtendedFields(String extendedField, AtomicLong extendedFieldValue) {
        this.extendedFields.put(extendedField, extendedFieldValue);
    }

    public Map<String, AtomicLong> getExtendedFields() {
        return extendedFields;
    }

    public String getStatsName() {
        return statsName;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SUCCESS_COUNT = "success_count";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String FAILED_COUNT = "failed_count";
    }
}
