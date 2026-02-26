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

package org.opensearch.action.admin.cluster.snapshots.delete;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Delete snapshot request
 * <p>
 * Delete snapshot request removes snapshots from the repository and cleans up all files that are associated with the snapshots.
 * All files that are shared with at least one other existing snapshot are left intact.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DeleteSnapshotRequest extends ClusterManagerNodeRequest<DeleteSnapshotRequest> {

    private String repository;

    private String[] snapshots;

    private boolean waitForCompletion;

    /**
     * Constructs a new delete snapshots request
     */
    public DeleteSnapshotRequest() {}

    /**
     * Constructs a new delete snapshots request with repository and snapshot names
     *
     * @param repository repository name
     * @param snapshots  snapshot names
     */
    public DeleteSnapshotRequest(String repository, String... snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new delete snapshots request with repository name
     *
     * @param repository repository name
     */
    public DeleteSnapshotRequest(String repository) {
        this.repository = repository;
    }

    public DeleteSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshots = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            waitForCompletion = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
        if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            out.writeBoolean(waitForCompletion);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshots == null || snapshots.length == 0) {
            validationException = addValidationError("snapshots are missing", validationException);
        }
        return validationException;
    }

    public DeleteSnapshotRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Returns snapshot names
     *
     * @return snapshot names
     */
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets snapshot names
     *
     * @return this request
     */
    public DeleteSnapshotRequest snapshots(String... snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    /**
     * If set to true the operation should wait for the snapshot deletion to complete before returning.
     * <p>
     * By default, the operation will return as soon as the deletion is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot deletion to complete
     * @return this request
     */
    public DeleteSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot deletion to complete before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteSnapshotRequest that = (DeleteSnapshotRequest) o;
        return waitForCompletion == that.waitForCompletion
            && Objects.equals(repository, that.repository)
            && Arrays.equals(snapshots, that.snapshots);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(repository, waitForCompletion);
        result = 31 * result + Arrays.hashCode(snapshots);
        return result;
    }
}
