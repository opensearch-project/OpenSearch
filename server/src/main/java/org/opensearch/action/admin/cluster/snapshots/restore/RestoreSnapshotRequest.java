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

package org.opensearch.action.admin.cluster.snapshots.restore;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Restore snapshot request
 *
 * @opensearch.internal
 */
public class RestoreSnapshotRequest extends ClusterManagerNodeRequest<RestoreSnapshotRequest> implements ToXContentObject {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestoreSnapshotRequest.class);

    /**
     * Enumeration of possible storage types
     */
    public enum StorageType {
        LOCAL("local"),
        REMOTE_SNAPSHOT("remote_snapshot");

        private final String text;

        StorageType(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }

        private void toXContent(XContentBuilder builder) throws IOException {
            builder.field("storage_type", text);
        }

        private static StorageType fromString(String string) {
            for (StorageType type : values()) {
                if (type.text.equals(string)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid storage_type: " + string);
        }
    }

    private String snapshot;
    private String repository;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String renamePattern;
    private String renameReplacement;
    private boolean waitForCompletion;
    private boolean includeGlobalState = false;
    private boolean partial = false;
    private boolean includeAliases = true;
    private Settings indexSettings = EMPTY_SETTINGS;
    private String[] ignoreIndexSettings = Strings.EMPTY_ARRAY;
    private StorageType storageType = StorageType.LOCAL;
    @Nullable
    private String sourceRemoteStoreRepository = null;

    @Nullable // if any snapshot UUID will do
    private String snapshotUuid;

    public RestoreSnapshotRequest() {}

    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public RestoreSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    public RestoreSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        renamePattern = in.readOptionalString();
        renameReplacement = in.readOptionalString();
        waitForCompletion = in.readBoolean();
        includeGlobalState = in.readBoolean();
        partial = in.readBoolean();
        includeAliases = in.readBoolean();
        indexSettings = readSettingsFromStream(in);
        ignoreIndexSettings = in.readStringArray();
        snapshotUuid = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            storageType = in.readEnum(StorageType.class);
        }
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE) && in.getVersion().onOrAfter(Version.V_2_9_0)) {
            sourceRemoteStoreRepository = in.readOptionalString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(renamePattern);
        out.writeOptionalString(renameReplacement);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(partial);
        out.writeBoolean(includeAliases);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoreIndexSettings);
        out.writeOptionalString(snapshotUuid);
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeEnum(storageType);
        }
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE) && out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeOptionalString(sourceRemoteStoreRepository);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices are missing", validationException);
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is missing", validationException);
        }
        if (indexSettings == null) {
            validationException = addValidationError("indexSettings are missing", validationException);
        }
        if (ignoreIndexSettings == null) {
            validationException = addValidationError("ignoreIndexSettings are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the snapshot.
     *
     * @param snapshot snapshot name
     * @return this request
     */
    public RestoreSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * Returns the name of the snapshot.
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public RestoreSnapshotRequest repository(String repository) {
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
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[0]);
        return this;
    }

    /**
     * Returns list of indices that should be restored from snapshot
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expression
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return this request
     */
    public RestoreSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets rename pattern that should be applied to restored indices.
     * <p>
     * Indices that match the rename pattern will be renamed according to {@link #renameReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more indices will be renamed into the same name.
     *
     * @param renamePattern rename pattern
     * @return this request
     */
    public RestoreSnapshotRequest renamePattern(String renamePattern) {
        this.renamePattern = renamePattern;
        return this;
    }

    /**
     * Returns rename pattern
     *
     * @return rename pattern
     */
    public String renamePattern() {
        return renamePattern;
    }

    /**
     * Sets rename replacement
     * <p>
     * See {@link #renamePattern(String)} for more information.
     *
     * @param renameReplacement rename replacement
     */
    public RestoreSnapshotRequest renameReplacement(String renameReplacement) {
        this.renameReplacement = renameReplacement;
        return this;
    }

    /**
     * Returns rename replacement
     *
     * @return rename replacement
     */
    public String renameReplacement() {
        return renameReplacement;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public RestoreSnapshotRequest waitForCompletion(boolean waitForCompletion) {
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
     * Returns true if indices with failed to snapshot shards should be partially restored.
     *
     * @return true if indices with failed to snapshot shards should be partially restored
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with failed to snapshot shards should be partially restored.
     *
     * @param partial true if indices with failed to snapshot shards should be partially restored.
     * @return this request
     */
    public RestoreSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(String... ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(List<String> ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings.toArray(new String[0]);
        return this;
    }

    /**
     * Returns the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public String[] ignoreIndexSettings() {
        return ignoreIndexSettings;
    }

    /**
     * If set to true the restore procedure will restore global cluster state.
     * <p>
     * The global cluster state includes persistent settings and index template definitions.
     *
     * @param includeGlobalState true if global state should be restored from the snapshot
     * @return this request
     */
    public RestoreSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be restored from this snapshot
     *
     * @return true if global state should be restored
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * If set to true the restore procedure will restore aliases
     *
     * @param includeAliases true if aliases should be restored from the snapshot
     * @return this request
     */
    public RestoreSnapshotRequest includeAliases(boolean includeAliases) {
        this.includeAliases = includeAliases;
        return this;
    }

    /**
     * Returns true if aliases should be restored from this snapshot
     *
     * @return true if aliases should be restored
     */
    public boolean includeAliases() {
        return includeAliases;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings settings) {
        this.indexSettings = settings;
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings.Builder settings) {
        this.indexSettings = settings.build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(String source, XContentType xContentType) {
        this.indexSettings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Map<String, Object> source) {
        this.indexSettings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Returns settings that should be added/changed in all restored indices
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    /**
     * Sometimes a client has identified precisely which snapshot is to be restored via a separate mechanism and wishes to guarantee that
     * this is the snapshot that this request restores. If the client can only identify a snapshot by its name then there is a risk that the
     * desired snapshot may be deleted and replaced by a new snapshot with the same name which is inconsistent with the original one. This
     * method lets us fail the restore if the precise snapshot we want is not available.
     *
     * This is for internal use only and is not exposed in the REST layer.
     */
    public RestoreSnapshotRequest snapshotUuid(String snapshotUuid) {
        this.snapshotUuid = snapshotUuid;
        return this;
    }

    /**
     * @return the UUID that identifies the specific snapshot in the repository to be restored, or {@code null} if the snapshot name is
     * a sufficient identifier.
     */
    @Nullable
    public String snapshotUuid() {
        return snapshotUuid;
    }

    /**
     * Sets the storage type for this request.
     */
    public RestoreSnapshotRequest storageType(StorageType storageType) {
        this.storageType = storageType;
        return this;
    }

    /**
     * Gets the storage type for this request. {@link StorageType#LOCAL} is the
     * implicit default if not overridden.
     */
    public StorageType storageType() {
        return storageType;
    }

    /**
     * Sets Source Remote Store Repository for all the restored indices
     *
     * @param sourceRemoteStoreRepository name of the remote store repository that should be used for all restored indices.
     */
    public RestoreSnapshotRequest setSourceRemoteStoreRepository(String sourceRemoteStoreRepository) {
        this.sourceRemoteStoreRepository = sourceRemoteStoreRepository;
        return this;
    }

    /**
     * Returns Source Remote Store Repository for all the restored indices
     *
     * @return source Remote Store Repository
     */
    public String getSourceRemoteStoreRepository() {
        return sourceRemoteStoreRepository;
    }

    /**
     * Parses restore definition
     *
     * @param source restore definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public RestoreSnapshotRequest source(Map<String, Object> source) {
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
            } else if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue(), "partial"));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed settings section");
                }
                DEPRECATION_LOGGER.deprecate(
                    "RestoreSnapshotRequest#settings",
                    "specifying [settings] when restoring a snapshot has no effect and will not be supported in a future version"
                );
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue(), "include_global_state");
            } else if (name.equals("include_aliases")) {
                includeAliases = nodeBooleanValue(entry.getValue(), "include_aliases");
            } else if (name.equals("rename_pattern")) {
                if (entry.getValue() instanceof String) {
                    renamePattern((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed rename_pattern");
                }
            } else if (name.equals("rename_replacement")) {
                if (entry.getValue() instanceof String) {
                    renameReplacement((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed rename_replacement");
                }
            } else if (name.equals("index_settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed index_settings section");
                }
                indexSettings((Map<String, Object>) entry.getValue());
            } else if (name.equals("ignore_index_settings")) {
                if (entry.getValue() instanceof String) {
                    ignoreIndexSettings(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    ignoreIndexSettings((List<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed ignore_index_settings section, should be an array of strings");
                }
            } else if (name.equals("storage_type")) {

                if (entry.getValue() instanceof String) {
                    storageType(StorageType.fromString((String) entry.getValue()));
                } else {
                    throw new IllegalArgumentException("malformed storage_type");
                }

            } else if (name.equals("source_remote_store_repository")) {
                if (!FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE)) {
                    throw new IllegalArgumentException(
                        "Unsupported parameter " + name + ". Please enable remote store feature flag for this experimental feature"
                    );
                }
                if (entry.getValue() instanceof String) {
                    setSourceRemoteStoreRepository((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed source_remote_store_repository");
                }
            } else {
                if (IndicesOptions.isIndicesOptions(name) == false) {
                    throw new IllegalArgumentException("Unknown parameter " + name);
                }
            }
        }
        indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
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
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        if (renamePattern != null) {
            builder.field("rename_pattern", renamePattern);
        }
        if (renameReplacement != null) {
            builder.field("rename_replacement", renameReplacement);
        }
        builder.field("include_global_state", includeGlobalState);
        builder.field("partial", partial);
        builder.field("include_aliases", includeAliases);
        if (indexSettings != null) {
            builder.startObject("index_settings");
            if (indexSettings.isEmpty() == false) {
                indexSettings.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.startArray("ignore_index_settings");
        for (String ignoreIndexSetting : ignoreIndexSettings) {
            builder.value(ignoreIndexSetting);
        }
        builder.endArray();
        if (storageType != null) {
            storageType.toXContent(builder);
        }
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE) && sourceRemoteStoreRepository != null) {
            builder.field("source_remote_store_repository", sourceRemoteStoreRepository);
        }
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
        RestoreSnapshotRequest that = (RestoreSnapshotRequest) o;
        boolean equals = waitForCompletion == that.waitForCompletion
            && includeGlobalState == that.includeGlobalState
            && partial == that.partial
            && includeAliases == that.includeAliases
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(renamePattern, that.renamePattern)
            && Objects.equals(renameReplacement, that.renameReplacement)
            && Objects.equals(indexSettings, that.indexSettings)
            && Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings)
            && Objects.equals(snapshotUuid, that.snapshotUuid)
            && Objects.equals(storageType, that.storageType);
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE)) {
            equals = Objects.equals(sourceRemoteStoreRepository, that.sourceRemoteStoreRepository);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        int result;
        if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE)) {
            result = Objects.hash(
                snapshot,
                repository,
                indicesOptions,
                renamePattern,
                renameReplacement,
                waitForCompletion,
                includeGlobalState,
                partial,
                includeAliases,
                indexSettings,
                snapshotUuid,
                storageType,
                sourceRemoteStoreRepository
            );
        } else {
            result = Objects.hash(
                snapshot,
                repository,
                indicesOptions,
                renamePattern,
                renameReplacement,
                waitForCompletion,
                includeGlobalState,
                partial,
                includeAliases,
                indexSettings,
                snapshotUuid,
                storageType
            );
        }
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return org.opensearch.common.Strings.toString(XContentType.JSON, this);
    }
}
