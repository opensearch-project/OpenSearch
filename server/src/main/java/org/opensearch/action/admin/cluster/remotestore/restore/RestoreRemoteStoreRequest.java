/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Restore remote store request
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class RestoreRemoteStoreRequest extends ClusterManagerNodeRequest<RestoreRemoteStoreRequest> implements ToXContentObject {

    private String[] indices = Strings.EMPTY_ARRAY;
    private Boolean waitForCompletion = false;
    private Boolean restoreAllShards = false;

    public RestoreRemoteStoreRequest() {}

    public RestoreRemoteStoreRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        waitForCompletion = in.readOptionalBoolean();
        restoreAllShards = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeOptionalBoolean(waitForCompletion);
        out.writeOptionalBoolean(restoreAllShards);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("indices are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the list of indices that should be restored from the remote store
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the cluster.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreRemoteStoreRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets the list of indices that should be restored from the remote store
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the cluster.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreRemoteStoreRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[0]);
        return this;
    }

    /**
     * Returns list of indices that should be restored from the remote store
     */
    public String[] indices() {
        return indices;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public RestoreRemoteStoreRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Set the value for restoreAllShards, denoting whether to restore all shards or only unassigned shards
     *
     * @param restoreAllShards If true, the operation will restore all the shards of the given indices.
     *                         If false, the operation will restore only the unassigned shards of the given indices.
     * @return this request
     */
    public RestoreRemoteStoreRequest restoreAllShards(boolean restoreAllShards) {
        this.restoreAllShards = restoreAllShards;
        return this;
    }

    /**
     * Returns restoreAllShards setting
     *
     * @return true if the operation will restore all the shards of the given indices
     */
    public boolean restoreAllShards() {
        return restoreAllShards;
    }

    /**
     * Parses restore definition
     *
     * @param source restore definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public RestoreRemoteStoreRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("indices")) {
                if (entry.getValue() instanceof String) {
                    indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof ArrayList) {
                    indices((ArrayList<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                }
            } else {
                if (IndicesOptions.isIndicesOptions(name) == false) {
                    throw new IllegalArgumentException("Unknown parameter " + name);
                }
            }
        }
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("indices");
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "remote_store";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreRemoteStoreRequest that = (RestoreRemoteStoreRequest) o;
        return waitForCompletion == that.waitForCompletion
            && restoreAllShards == that.restoreAllShards
            && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(waitForCompletion, restoreAllShards);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

}
