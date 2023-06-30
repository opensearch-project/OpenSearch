/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.monitor.process;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;

/**
 * Holds information for monitoring the process
*
* @opensearch.internal
*/
public class ProtobufProcessInfo implements ProtobufReportingService.ProtobufInfo {

    private final long refreshInterval;
    private final long id;
    private final boolean mlockall;

    public ProtobufProcessInfo(long id, boolean mlockall, long refreshInterval) {
        this.id = id;
        this.mlockall = mlockall;
        this.refreshInterval = refreshInterval;
    }

    public ProtobufProcessInfo(CodedInputStream in) throws IOException {
        refreshInterval = in.readInt64();
        id = in.readInt64();
        mlockall = in.readBool();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(refreshInterval);
        out.writeInt64NoTag(id);
        out.writeBoolNoTag(mlockall);
    }

    public long refreshInterval() {
        return this.refreshInterval;
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    /**
     * The process id.
    */
    public long getId() {
        return id;
    }

    public boolean isMlockall() {
        return mlockall;
    }

    static final class Fields {
        static final String PROCESS = "process";
        static final String REFRESH_INTERVAL = "refresh_interval";
        static final String REFRESH_INTERVAL_IN_MILLIS = "refresh_interval_in_millis";
        static final String ID = "id";
        static final String MLOCKALL = "mlockall";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PROCESS);
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, new TimeValue(refreshInterval));
        builder.field(Fields.ID, id);
        builder.field(Fields.MLOCKALL, mlockall);
        builder.endObject();
        return builder;
    }
}
