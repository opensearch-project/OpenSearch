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

package org.opensearch.discovery;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.cluster.coordination.ProtobufPendingClusterStateStats;
import org.opensearch.cluster.coordination.ProtobufPublishClusterStateStats;

import java.io.IOException;

/**
 * Stores discovery stats
*
* @opensearch.internal
*/
public class ProtobufDiscoveryStats implements ProtobufWriteable, ToXContentFragment {

    private final ProtobufPendingClusterStateStats queueStats;
    private final ProtobufPublishClusterStateStats publishStats;

    public ProtobufDiscoveryStats(ProtobufPendingClusterStateStats queueStats, ProtobufPublishClusterStateStats publishStats) {
        this.queueStats = queueStats;
        this.publishStats = publishStats;
    }

    public ProtobufDiscoveryStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        queueStats = protobufStreamInput.readOptionalWriteable(ProtobufPendingClusterStateStats::new);
        publishStats = protobufStreamInput.readOptionalWriteable(ProtobufPublishClusterStateStats::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(queueStats);
        protobufStreamOutput.writeOptionalWriteable(publishStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DISCOVERY);
        if (queueStats != null) {
            queueStats.toXContent(builder, params);
        }
        if (publishStats != null) {
            publishStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String DISCOVERY = "discovery";
    }

    public ProtobufPendingClusterStateStats getQueueStats() {
        return queueStats;
    }

    public ProtobufPublishClusterStateStats getPublishStats() {
        return publishStats;
    }
}
