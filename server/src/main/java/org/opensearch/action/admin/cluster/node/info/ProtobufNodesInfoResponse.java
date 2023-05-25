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
*     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.action.support.nodes.ProtobufBaseNodesResponse;
import org.opensearch.cluster.ProtobufClusterName;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Transport response for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufNodesInfoResponse extends ProtobufBaseNodesResponse<ProtobufNodeInfo> {

    public ProtobufNodesInfoResponse(CodedInputStream in) throws IOException {
        super(in);
    }

    public ProtobufNodesInfoResponse(
        ProtobufClusterName clusterName,
        List<ProtobufNodeInfo> nodes,
        List<ProtobufFailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ProtobufNodeInfo> readNodesFrom(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        return protobufStreamInput.readList(ProtobufNodeInfo::new);
    }

    @Override
    protected void writeNodesTo(CodedOutputStream out, List<ProtobufNodeInfo> nodes) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeCollection(nodes, (o, v) -> v.writeTo(o));
    }
}
