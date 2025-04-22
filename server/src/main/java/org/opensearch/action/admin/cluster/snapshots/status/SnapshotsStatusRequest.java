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

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot status request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SnapshotsStatusRequest extends ClusterManagerNodeRequest<SnapshotsStatusRequest> {

    private String repository = "_all";

    private String[] snapshots = Strings.EMPTY_ARRAY;
    private String[] indices = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    public SnapshotsStatusRequest() {}

    /**
     * Constructs a new get snapshots request with given repository name and list of snapshots
     *
     * @param repository repository name
     * @param snapshots  list of snapshots
     */
    public SnapshotsStatusRequest(String repository, String[] snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new get snapshots request with given repository name and list of snapshots
     *
     * @param repository repository name
     * @param snapshots  list of snapshots
     * @param indices  list of indices
     */
    public SnapshotsStatusRequest(String repository, String[] snapshots, String[] indices) {
        this.repository = repository;
        this.snapshots = snapshots;
        this.indices = indices;
    }

    public SnapshotsStatusRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            indices = in.readOptionalStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
        out.writeBoolean(ignoreUnavailable);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeOptionalStringArray(indices);
        }
    }

    /**
     * Constructs a new get snapshots request with given repository name
     *
     * @param repository repository name
     */
    public SnapshotsStatusRequest(String repository) {
        this.repository = repository;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshots == null) {
            validationException = addValidationError("snapshots is null", validationException);
        }
        if (indices.length != 0) {
            if (repository.equals("_all")) {
                String error =
                    "index list filter is supported only when a single 'repository' is passed, but found 'repository' param = [_all]";
                validationException = addValidationError(error, validationException);
            }
            if (snapshots.length != 1) {
                // snapshot param was '_all' (length = 0) or a list of snapshots (length > 1)
                String snapshotParamValue = snapshots.length == 0 ? "_all" : Arrays.toString(snapshots);
                String error = "index list filter is supported only when a single 'snapshot' is passed, but found 'snapshot' param = ["
                    + snapshotParamValue
                    + "]";
                validationException = addValidationError(error, validationException);
            }
        }
        return validationException;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public SnapshotsStatusRequest repository(String repository) {
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
     * Returns the names of the snapshots.
     *
     * @return the names of snapshots
     */
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets the list of snapshots to be returned
     *
     * @return this request
     */
    public SnapshotsStatusRequest snapshots(String[] snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    /**
     * Returns the names of the indices.
     *
     * @return the names of indices
     */
    public String[] indices() {
        return this.indices;
    }

    /**
     * Sets the list of indices to be returned
     *
     * @return this request
     */
    public SnapshotsStatusRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Set to <code>true</code> to ignore unavailable snapshots and indices, instead of throwing an exception.
     * Defaults to <code>false</code>, which means unavailable snapshots and indices cause an exception to be thrown.
     *
     * @param ignoreUnavailable whether to ignore unavailable snapshots and indices
     * @return this request
     */
    public SnapshotsStatusRequest ignoreUnavailable(boolean ignoreUnavailable) {
        this.ignoreUnavailable = ignoreUnavailable;
        return this;
    }

    /**
     * Returns whether the request permits unavailable snapshots and indices to be ignored.
     *
     * @return true if the request will ignore unavailable snapshots and indices, false if it will throw an exception on unavailable snapshots and indices
     */
    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }
}
