/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.admin.cluster.state;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ProtobufClusterManagerNodeReadRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Transport request for obtaining cluster state
*
* @opensearch.internal
*/
public class ProtobufClusterStateRequest extends ProtobufClusterManagerNodeReadRequest<ProtobufClusterStateRequest>
    implements
        IndicesRequest.Replaceable {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metadata = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetadataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    public ProtobufClusterStateRequest() {}

    public ProtobufClusterStateRequest(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        routingTable = in.readBool();
        nodes = in.readBool();
        metadata = in.readBool();
        blocks = in.readBool();
        customs = in.readBool();
        indices = protobufStreamInput.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptionsProtobuf(in);
        waitForTimeout = protobufStreamInput.readTimeValue();
        waitForMetadataVersion = protobufStreamInput.readOptionalLong();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeBoolNoTag(routingTable);
        out.writeBoolNoTag(nodes);
        out.writeBoolNoTag(metadata);
        out.writeBoolNoTag(blocks);
        out.writeBoolNoTag(customs);
        protobufStreamOutput.writeStringArray(indices);
        indicesOptions.writeIndicesOptionsProtobuf(out);
        protobufStreamOutput.writeTimeValue(waitForTimeout);
        protobufStreamOutput.writeOptionalLong(waitForMetadataVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ProtobufClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metadata = true;
        blocks = true;
        customs = true;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public ProtobufClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metadata = false;
        blocks = false;
        customs = false;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ProtobufClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ProtobufClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metadata() {
        return metadata;
    }

    public ProtobufClusterStateRequest metadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ProtobufClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ProtobufClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public final ProtobufClusterStateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public ProtobufClusterStateRequest customs(boolean customs) {
        this.customs = customs;
        return this;
    }

    public boolean customs() {
        return customs;
    }

    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    public ProtobufClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.waitForTimeout = waitForTimeout;
        return this;
    }

    public Long waitForMetadataVersion() {
        return waitForMetadataVersion;
    }

    public ProtobufClusterStateRequest waitForMetadataVersion(long waitForMetadataVersion) {
        if (waitForMetadataVersion < 1) {
            throw new IllegalArgumentException(
                "provided waitForMetadataVersion should be >= 1, but instead is [" + waitForMetadataVersion + "]"
            );
        }
        this.waitForMetadataVersion = waitForMetadataVersion;
        return this;
    }
}
