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

package org.opensearch.transport;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
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
public final class RemoteConnectionInfo implements ToXContentFragment, Writeable {

    final ModeInfo modeInfo;
    final TimeValue initialConnectionTimeout;
    final String clusterAlias;
    final boolean skipUnavailable;

    public RemoteConnectionInfo(String clusterAlias, ModeInfo modeInfo, TimeValue initialConnectionTimeout, boolean skipUnavailable) {
        this.clusterAlias = clusterAlias;
        this.modeInfo = modeInfo;
        this.initialConnectionTimeout = initialConnectionTimeout;
        this.skipUnavailable = skipUnavailable;
    }

    public RemoteConnectionInfo(StreamInput input) throws IOException {
        RemoteConnectionStrategy.ConnectionStrategy mode = input.readEnum(RemoteConnectionStrategy.ConnectionStrategy.class);
        modeInfo = mode.getReader().read(input);
        initialConnectionTimeout = input.readTimeValue();
        clusterAlias = input.readString();
        skipUnavailable = input.readBoolean();
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(modeInfo.modeType());
        modeInfo.writeTo(out);
        out.writeTimeValue(initialConnectionTimeout);
        out.writeString(clusterAlias);
        out.writeBoolean(skipUnavailable);
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
        RemoteConnectionInfo that = (RemoteConnectionInfo) o;
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
    public interface ModeInfo extends ToXContentFragment, Writeable {

        boolean isConnected();

        String modeName();

        RemoteConnectionStrategy.ConnectionStrategy modeType();
    }
}
