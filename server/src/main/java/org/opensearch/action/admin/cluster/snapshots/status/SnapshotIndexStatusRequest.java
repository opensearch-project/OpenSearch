/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request for listing snapshot index statuses with pagination.
 * Targets a single, specific snapshot within a single repository.
 *
 * @opensearch.internal
 */
public class SnapshotIndexStatusRequest extends ClusterManagerNodeRequest<SnapshotIndexStatusRequest> {

    public static final int DEFAULT_FROM = 0;
    public static final int DEFAULT_SIZE = 20;
    public static final int MAX_SIZE = 200;

    private String repository;
    private String snapshot;
    private int from = DEFAULT_FROM;
    private int size = DEFAULT_SIZE;

    public SnapshotIndexStatusRequest() {}

    public SnapshotIndexStatusRequest(String repository, String snapshot) {
        this.repository = repository;
        this.snapshot = snapshot;
    }

    public SnapshotIndexStatusRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshot = in.readString();
        from = in.readVInt();
        size = in.readVInt();
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
        ActionRequestValidationException validationException = null;
        if (repository == null || repository.isEmpty()) {
            validationException = addValidationError("repository is missing", validationException);
        } else if ("_all".equals(repository)) {
            validationException = addValidationError("repository must be a specific repository name, not [_all]", validationException);
        }
        if (snapshot == null || snapshot.isEmpty()) {
            validationException = addValidationError("snapshot is missing", validationException);
        } else if ("_all".equals(snapshot) || "_current".equals(snapshot)) {
            validationException = addValidationError(
                "snapshot must be a specific snapshot name, not [" + snapshot + "]",
                validationException
            );
        }
        if (from < 0) {
            validationException = addValidationError("from must be >= 0 but was [" + from + "]", validationException);
        }
        if (size < 1 || size > MAX_SIZE) {
            validationException = addValidationError(
                "size must be between 1 and " + MAX_SIZE + " but was [" + size + "]",
                validationException
            );
        }
        return validationException;
    }

    public String repository() {
        return repository;
    }

    public SnapshotIndexStatusRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    public String snapshot() {
        return snapshot;
    }

    public SnapshotIndexStatusRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public int from() {
        return from;
    }

    public SnapshotIndexStatusRequest from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public SnapshotIndexStatusRequest size(int size) {
        this.size = size;
        return this;
    }
}
