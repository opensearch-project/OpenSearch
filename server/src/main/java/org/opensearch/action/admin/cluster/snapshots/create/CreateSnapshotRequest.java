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

package org.opensearch.action.admin.cluster.snapshots.create;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;
import static org.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.opensearch.core.common.Strings.EMPTY_ARRAY;
import static org.opensearch.snapshots.SnapshotInfo.METADATA_FIELD_INTRODUCED;

/**
 * Create snapshot request
 * <p>
 * The only mandatory parameter is repository name. The repository name has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('_')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file name characters {@link Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateSnapshotRequest extends ClusterManagerNodeRequest<CreateSnapshotRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {
    public static int MAXIMUM_METADATA_BYTES = 1024; // chosen arbitrarily

    private String snapshot;

    private String repository;

    private String[] indices = EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenHidden();

    private boolean partial = false;

    private Settings settings = EMPTY_SETTINGS;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    private Map<String, Object> userMetadata;

    public CreateSnapshotRequest() {}

    /**
     * Constructs a new put repository request with the provided snapshot and repository names
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public CreateSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    public CreateSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        includeGlobalState = in.readBoolean();
        waitForCompletion = in.readBoolean();
        partial = in.readBoolean();
        if (in.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
            userMetadata = in.readMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(settings, out);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
        if (out.getVersion().onOrAfter(METADATA_FIELD_INTRODUCED)) {
            out.writeMap(userMetadata);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("snapshot is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }
        if (settings == null) {
            validationException = addValidationError("settings is null", validationException);
        }
        final int metadataSize = metadataSize(userMetadata);
        if (metadataSize > MAXIMUM_METADATA_BYTES) {
            validationException = addValidationError(
                "metadata must be smaller than 1024 bytes, but was [" + metadataSize + "]",
                validationException
            );
        }
        return validationException;
    }

    public static int metadataSize(Map<String, Object> userMetadata) {
        if (userMetadata == null) {
            return 0;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.value(userMetadata);
            int size = BytesReference.bytes(builder).length();
            return size;
        } catch (IOException e) {
            // This should not be possible as we are just rendering the xcontent in memory
            throw new OpenSearchException(e);
        }
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     */
    public CreateSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * The snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository name
     * @return this request
     */
    public CreateSnapshotRequest repository(String repository) {
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
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this request
     */
    @Override
    public CreateSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this request
     */
    public CreateSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[0]);
        return this;
    }

    /**
     * Returns a list of indices that should be included into the snapshot
     *
     * @return array of index names
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices options
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns true if indices with unavailable shards should be be partially snapshotted.
     *
     * @return the desired behaviour regarding indices options
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with unavailable shards to be partially snapshotted.
     *
     * @param partial true if indices with unavailable shards should be be partially snapshotted.
     * @return this request
     */
    public CreateSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * If set to true the operation should wait for the snapshot completion before returning.
     * <p>
     * By default, the operation will return as soon as snapshot is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot completion
     * @return this request
     */
    public CreateSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings in JSON or YAML format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @param mediaType the content type of the source
     * @return this request
     */
    public CreateSnapshotRequest settings(String source, MediaType mediaType) {
        this.settings = Settings.builder().loadFromSource(source, mediaType).build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
            builder.map(source);
            settings(builder.toString(), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns repository-specific snapshot settings
     *
     * @return repository-specific snapshot settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Set to true if global state should be stored as part of the snapshot
     *
     * @param includeGlobalState true if global state should be stored
     * @return this request
     */
    public CreateSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be stored as part of the snapshot
     *
     * @return true if global state should be stored as part of the snapshot
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    public Map<String, Object> userMetadata() {
        return userMetadata;
    }

    public CreateSnapshotRequest userMetadata(Map<String, Object> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    /**
     * Parses snapshot definition.
     *
     * @param source snapshot definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public CreateSnapshotRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("indices")) {
                if (entry.getValue() instanceof String) {
                    indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    indices((List<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                }
            } else if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue(), "partial"));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed settings section, should indices an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue(), "include_global_state");
            } else if (name.equals("metadata")) {
                if (entry.getValue() != null && (entry.getValue() instanceof Map == false)) {
                    throw new IllegalArgumentException("malformed metadata, should be an object");
                }
                userMetadata((Map<String, Object>) entry.getValue());
            }
        }
        indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("snapshot", snapshot);
        builder.startArray("indices");
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.field("partial", partial);
        if (settings != null) {
            builder.startObject("settings");
            if (settings.isEmpty() == false) {
                settings.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.field("include_global_state", includeGlobalState);
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        builder.field("metadata", userMetadata);
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotRequest that = (CreateSnapshotRequest) o;
        return partial == that.partial
            && includeGlobalState == that.includeGlobalState
            && waitForCompletion == that.waitForCompletion
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(settings, that.settings)
            && Objects.equals(clusterManagerNodeTimeout, that.clusterManagerNodeTimeout)
            && Objects.equals(userMetadata, that.userMetadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            snapshot,
            repository,
            indicesOptions,
            partial,
            settings,
            includeGlobalState,
            waitForCompletion,
            userMetadata
        );
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return "CreateSnapshotRequest{"
            + "snapshot='"
            + snapshot
            + '\''
            + ", repository='"
            + repository
            + '\''
            + ", indices="
            + (indices == null ? null : Arrays.asList(indices))
            + ", indicesOptions="
            + indicesOptions
            + ", partial="
            + partial
            + ", settings="
            + settings
            + ", includeGlobalState="
            + includeGlobalState
            + ", waitForCompletion="
            + waitForCompletion
            + ", clusterManagerNodeTimeout="
            + clusterManagerNodeTimeout
            + ", metadata="
            + userMetadata
            + '}';
    }
}
