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

package org.opensearch.action.admin.cluster.snapshots.clone;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for cloning a snapshot
 *
 * @opensearch.internal
 */
public class CloneSnapshotRequest extends ClusterManagerNodeRequest<CloneSnapshotRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    private final String repository;

    private final String source;

    private final String target;

    private String[] indices;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();

    public CloneSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        source = in.readString();
        target = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    /**
     * Creates a clone snapshot request for cloning the given source snapshot's indices into the given target snapshot on the given
     * repository.
     *
     * @param repository repository that source snapshot belongs to and that the target snapshot will be created in
     * @param source     source snapshot name
     * @param target     target snapshot name
     * @param indices    indices to clone from source to target
     */
    public CloneSnapshotRequest(String repository, String source, String target, String[] indices) {
        this.repository = repository;
        this.source = source;
        this.target = target;
        this.indices = indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(source);
        out.writeString(target);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("source snapshot name is missing", null);
        }
        if (target == null) {
            validationException = addValidationError("target snapshot name is missing", null);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else if (indices.length == 0) {
            validationException = addValidationError("indices patterns are empty", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public CloneSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * @see CloneSnapshotRequestBuilder#setIndicesOptions
     */
    public CloneSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public String repository() {
        return this.repository;
    }

    public String target() {
        return this.target;
    }

    public String source() {
        return this.source;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("source", source);
        builder.field("target", target);
        if (indices != null) {
            builder.startArray("indices");
            for (String index : indices) {
                builder.value(index);
            }
            builder.endArray();
        }
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }
}
