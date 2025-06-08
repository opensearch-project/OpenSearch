/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.io.IOException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

@ExperimentalApi
public class RemoteMetadataRef {

    public String refType;
    public String arn;

    public RemoteMetadataRef(String refType, String arn) {
        this.refType = refType;
        this.arn = arn;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(refType);
        out.writeString(arn);
    }

    public static RemoteMetadataRef readFrom(StreamInput in) throws IOException {
        return new RemoteMetadataRef(in.readString(),in.readString());
    }

    @Override
    public String toString() {
        return "ClusterStateRemoteRef{" + "refType='" + refType + '\'' + ", arn='" + arn + '\'' + '}';
    }
}
