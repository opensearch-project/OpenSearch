/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * This class encapsulates all remote cluster information to be rendered on
* {@code _remote/info} requests.
*
* @opensearch.internal
*/
public final class ProtobufRemoteConnectionInfo implements ToXContentFragment, ProtobufWriteable {

    final ModeInfo modeInfo;
    final TimeValue initialConnectionTimeout;
    final String clusterAlias;
    final boolean skipUnavailable;

    public ProtobufRemoteConnectionInfo(
        String clusterAlias,
        ModeInfo modeInfo,
        TimeValue initialConnectionTimeout,
        boolean skipUnavailable
    ) {
        this.clusterAlias = clusterAlias;
        this.modeInfo = modeInfo;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
    }

    public ProtobufRemoteConnectionInfo(CodedInputStream input) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(input);
        ProtobufRemoteConnectionStrategy.ProtobufConnectionStrategy mode = protobufStreamInput.readEnum(
            ProtobufRemoteConnectionStrategy.ProtobufConnectionStrategy.class
        );
        modeInfo = mode.getReader().read(input);
        initialConnectionTimeout = protobufStreamInput.readTimeValue();
        clusterAlias = input.readString();
        skipUnavailable = input.readBool();
    }

    public boolean isConnected() {
        return modeInfo.isConnected();
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    public ModeInfo getModeInfo() {
        return modeInfo;
    }

    public TimeValue getInitialConnectionTimeout() {
        return initialConnectionTimeout;
    }

    public boolean isSkipUnavailable() {
        return skipUnavailable;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeEnum(modeInfo.modeType());
        modeInfo.writeTo(out);
        protobufStreamOutput.writeTimeValue(initialConnectionTimeout);
        out.writeStringNoTag(clusterAlias);
        out.writeBoolNoTag(skipUnavailable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(clusterAlias);
        {
            builder.field("connected", modeInfo.isConnected());
            builder.field("mode", modeInfo.modeName());
            modeInfo.toXContent(builder, params);
            builder.field("initial_connect_timeout", initialConnectionTimeout);
            builder.field("skip_unavailable", skipUnavailable);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProtobufRemoteConnectionInfo that = (ProtobufRemoteConnectionInfo) o;
        return skipUnavailable == that.skipUnavailable
            && Objects.equals(modeInfo, that.modeInfo)
            && Objects.equals(initialConnectionTimeout, that.initialConnectionTimeout)
            && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modeInfo, initialConnectionTimeout, clusterAlias, skipUnavailable);
    }

    /**
     * Mode information
    *
    * @opensearch.internal
    */
    public interface ModeInfo extends ToXContentFragment, ProtobufWriteable {

        boolean isConnected();

        String modeName();

        ProtobufRemoteConnectionStrategy.ProtobufConnectionStrategy modeType();
    }
}
