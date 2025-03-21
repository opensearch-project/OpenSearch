/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Indicates ingestion failures at index and shard level.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IngestionStateShardFailure implements Writeable, ToXContentFragment {
    private static final String SHARD = "shard";
    private static final String ERROR = "error";

    private final String index;
    private final int shard;
    private String errorMessage;

    public IngestionStateShardFailure(String index, int shard, String errorMessage) {
        this.index = index;
        this.shard = shard;
        this.errorMessage = errorMessage;
    }

    public IngestionStateShardFailure(StreamInput in) throws IOException {
        this.index = in.readString();
        this.shard = in.readInt();
        this.errorMessage = in.readString();
    }

    public String getIndex() {
        return index;
    }

    public int getShard() {
        return shard;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeInt(shard);
        out.writeString(errorMessage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shard);
        builder.field(ERROR, errorMessage);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionStateShardFailure that = (IngestionStateShardFailure) o;
        return Objects.equals(index, that.index) && shard == that.shard && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(index);
        result = 31 * result + Objects.hashCode(shard);
        result = 31 * result + Objects.hashCode(errorMessage);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Groups provided shard ingestion state failures by index name.
     */
    public static Map<String, List<IngestionStateShardFailure>> groupShardFailuresByIndex(IngestionStateShardFailure[] shardFailures) {
        Map<String, List<IngestionStateShardFailure>> shardFailuresByIndex = new HashMap<>();

        for (IngestionStateShardFailure shardFailure : shardFailures) {
            shardFailuresByIndex.computeIfAbsent(shardFailure.getIndex(), (index) -> new ArrayList<>());
            shardFailuresByIndex.get(shardFailure.getIndex()).add(shardFailure);
        }

        return shardFailuresByIndex;
    }
}
