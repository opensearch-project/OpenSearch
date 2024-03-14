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

package org.opensearch.repositories;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Stats about a repository
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RepositoryStats implements Writeable, ToXContentFragment {

    public static final RepositoryStats EMPTY_STATS = new RepositoryStats(Collections.emptyMap());

    @Nullable
    public final Map<String, Long> requestCounts;
    @Nullable
    public final Map<BlobStore.Metric, Map<String, Long>> extendedStats;
    public final boolean detailed;

    public RepositoryStats(Map<String, Long> requestCounts) {
        this.requestCounts = Collections.unmodifiableMap(requestCounts);
        this.extendedStats = Collections.emptyMap();
        this.detailed = false;
    }

    public RepositoryStats(Map<BlobStore.Metric, Map<String, Long>> extendedStats, boolean detailed) {
        this.requestCounts = Collections.emptyMap();
        this.extendedStats = Collections.unmodifiableMap(extendedStats);
        this.detailed = detailed;
    }

    public RepositoryStats(StreamInput in) throws IOException {
        this.requestCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.extendedStats = in.readMap(
            e -> e.readEnum(BlobStore.Metric.class),
            i -> i.readMap(StreamInput::readString, StreamInput::readLong)
        );
        this.detailed = in.readBoolean();
    }

    public RepositoryStats merge(RepositoryStats otherStats) {
        assert this.detailed == otherStats.detailed;
        if (detailed) {
            final Map<BlobStore.Metric, Map<String, Long>> result = new HashMap<>();
            result.putAll(extendedStats);
            for (Map.Entry<BlobStore.Metric, Map<String, Long>> entry : otherStats.extendedStats.entrySet()) {
                for (Map.Entry<String, Long> nested : entry.getValue().entrySet()) {
                    result.get(entry.getKey()).merge(nested.getKey(), nested.getValue(), Math::addExact);
                }
            }
            return new RepositoryStats(result, true);
        } else {
            final Map<String, Long> result = new HashMap<>();
            result.putAll(requestCounts);
            for (Map.Entry<String, Long> entry : otherStats.requestCounts.entrySet()) {
                result.merge(entry.getKey(), entry.getValue(), Math::addExact);
            }
            return new RepositoryStats(result);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(requestCounts, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(extendedStats, StreamOutput::writeEnum, (o, v) -> o.writeMap(v, StreamOutput::writeString, StreamOutput::writeLong));
        out.writeBoolean(detailed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStats that = (RepositoryStats) o;
        return requestCounts.equals(that.requestCounts) && extendedStats.equals(that.extendedStats) && detailed == that.detailed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestCounts, detailed, extendedStats);
    }

    @Override
    public String toString() {
        return "RepositoryStats{" + "requestCounts=" + requestCounts + "extendedStats=" + extendedStats + "detailed =" + detailed + "}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (detailed == false) {
            builder.field("request_counts", requestCounts);
        } else {
            extendedStats.forEach((k, v) -> {
                try {
                    builder.field(k.metricName(), v);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return builder;
    }
}
