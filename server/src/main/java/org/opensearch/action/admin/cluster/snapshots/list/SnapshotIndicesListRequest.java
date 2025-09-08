/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.list;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for listing snapshot indices with pagination.
 *
 * @opensearch.internal
 */
public class SnapshotIndicesListRequest extends ClusterManagerNodeRequest<SnapshotIndicesListRequest> {

    private String repository;
    private String snapshot;
    private int from;
    private int size;

    public SnapshotIndicesListRequest() {}

    public SnapshotIndicesListRequest(StreamInput in) throws IOException {
        super(in);
        this.repository = in.readString();
        this.snapshot = in.readString();
        this.from = in.readVInt();
        this.size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(snapshot);
        out.writeVInt(from);
        out.writeVInt(size);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String repository() { return repository; }
    public String snapshot() { return snapshot; }
    public int from() { return from; }
    public int size() { return size; }

    public SnapshotIndicesListRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    public SnapshotIndicesListRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public SnapshotIndicesListRequest from(int from) {
        this.from = from;
        return this;
    }

    public SnapshotIndicesListRequest size(int size) {
        this.size = size;
        return this;
    }
}


