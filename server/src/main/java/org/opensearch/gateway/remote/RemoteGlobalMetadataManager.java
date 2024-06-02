/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.RemoteWritableEntityStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteGlobalMetadata;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A Manager which provides APIs to write and read Global Metadata attributes to remote store
 *
 * @opensearch.internal
 */
public class RemoteGlobalMetadataManager {

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 1;

    private volatile TimeValue globalMetadataUploadTimeout;
    private final RemoteWritableEntityStore<Metadata, RemoteGlobalMetadata> globalMetadataBlobStore;
    private final RemoteClusterStateBlobStore<CoordinationMetadata, RemoteCoordinationMetadata> coordinationMetadataBlobStore;
    private final RemoteClusterStateBlobStore<Settings, RemotePersistentSettingsMetadata> persistentSettingsBlobStore;
    private final RemoteClusterStateBlobStore<TemplatesMetadata, RemoteTemplatesMetadata> templatesMetadataBlobStore;
    private final RemoteClusterStateBlobStore<Custom, RemoteCustomMetadata> customMetadataBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;

    RemoteGlobalMetadataManager(
        ClusterSettings clusterSettings,
        RemoteWritableEntityStore<Metadata, RemoteGlobalMetadata> globalMetadataBlobStore,
        RemoteClusterStateBlobStore<CoordinationMetadata, RemoteCoordinationMetadata> coordinationMetadataBlobStore,
        RemoteClusterStateBlobStore<Settings, RemotePersistentSettingsMetadata> persistentSettingsBlobStore,
        RemoteClusterStateBlobStore<TemplatesMetadata, RemoteTemplatesMetadata> templatesMetadataBlobStore,
        RemoteClusterStateBlobStore<Custom, RemoteCustomMetadata> customMetadataBlobStore,
        Compressor compressor,
        NamedXContentRegistry namedXContentRegistry
    ) {
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.compressor = compressor;
        this.globalMetadataBlobStore = globalMetadataBlobStore;
        this.namedXContentRegistry = namedXContentRegistry;
        this.coordinationMetadataBlobStore = coordinationMetadataBlobStore;
        this.persistentSettingsBlobStore = persistentSettingsBlobStore;
        this.templatesMetadataBlobStore = templatesMetadataBlobStore;
        this.customMetadataBlobStore = customMetadataBlobStore;
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
    }

    /**
     * Allows async upload of Metadata components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        AbstractRemoteWritableBlobEntity writeEntity,
        RemoteWritableEntityStore remoteStore,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        return () -> remoteStore.writeAsync(writeEntity, getActionListener(writeEntity, latchedActionListener));
    }

    private ActionListener<Void> getActionListener(
        AbstractRemoteWritableBlobEntity remoteBlobStoreObject,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        return ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteBlobStoreObject.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException("Upload failed", ex))
        );
    }

    Metadata getGlobalMetadata(String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        String globalMetadataFileName = clusterMetadataManifest.getGlobalMetadataFileName();
        try {
            // Fetch Global metadata
            if (globalMetadataFileName != null) {
                RemoteGlobalMetadata remoteGlobalMetadata = new RemoteGlobalMetadata(
                    globalMetadataFileName,
                    clusterUUID,
                    compressor,
                    namedXContentRegistry
                );
                return globalMetadataBlobStore.read(remoteGlobalMetadata);
            } else if (clusterMetadataManifest.hasMetadataAttributesFiles()) {
                Metadata.Builder builder = new Metadata.Builder();
                if (clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename() != null) {
                    RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
                        clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.coordinationMetadata(coordinationMetadataBlobStore.read(remoteCoordinationMetadata));
                }
                if (clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename() != null) {
                    RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata(
                        clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.templates(templatesMetadataBlobStore.read(remoteTemplatesMetadata));
                }
                if (clusterMetadataManifest.getSettingsMetadata().getUploadedFilename() != null) {
                    RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata(
                        clusterMetadataManifest.getSettingsMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.persistentSettings(persistentSettingsBlobStore.read(remotePersistentSettingsMetadata));
                }
                builder.clusterUUID(clusterMetadataManifest.getClusterUUID());
                builder.clusterUUIDCommitted(clusterMetadataManifest.isClusterUUIDCommitted());
                clusterMetadataManifest.getCustomMetadataMap().forEach((key, value) -> {
                    try {
                        builder.putCustom(
                            key,
                            customMetadataBlobStore.read(
                                new RemoteCustomMetadata(value.getUploadedFilename(), key, clusterUUID, compressor, namedXContentRegistry)
                            )
                        );
                    } catch (IOException e) {
                        throw new IllegalStateException(
                            String.format(Locale.ROOT, "Error while downloading Custom Metadata - %s", value.getUploadedFilename()),
                            e
                        );
                    }
                });
                return builder.build();
            } else {
                return Metadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Global Metadata - %s", globalMetadataFileName),
                e
            );
        }
    }

    Map<String, Metadata.Custom> getUpdatedCustoms(ClusterState currentState, ClusterState previousState) {
        if (Metadata.isCustomMetadataEqual(previousState.metadata(), currentState.metadata())) {
            return new HashMap<>();
        }
        Map<String, Metadata.Custom> updatedCustom = new HashMap<>();
        Set<String> currentCustoms = new HashSet<>(currentState.metadata().customs().keySet());
        for (Map.Entry<String, Metadata.Custom> cursor : previousState.metadata().customs().entrySet()) {
            if (cursor.getValue().context().contains(Metadata.XContentContext.GATEWAY)) {
                if (currentCustoms.contains(cursor.getKey())
                    && !cursor.getValue().equals(currentState.metadata().custom(cursor.getKey()))) {
                    // If the custom metadata is updated, we need to upload the new version.
                    updatedCustom.put(cursor.getKey(), currentState.metadata().custom(cursor.getKey()));
                }
                currentCustoms.remove(cursor.getKey());
            }
        }
        for (String custom : currentCustoms) {
            Metadata.Custom cursor = currentState.metadata().custom(custom);
            if (cursor.context().contains(Metadata.XContentContext.GATEWAY)) {
                updatedCustom.put(custom, cursor);
            }
        }
        return updatedCustom;
    }

    boolean isGlobalMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        Metadata secondGlobalMetadata = getGlobalMetadata(second.getClusterUUID(), second);
        Metadata firstGlobalMetadata = getGlobalMetadata(first.getClusterUUID(), first);
        return Metadata.isGlobalResourcesMetadataEquals(firstGlobalMetadata, secondGlobalMetadata);
    }

    private void setGlobalMetadataUploadTimeout(TimeValue newGlobalMetadataUploadTimeout) {
        this.globalMetadataUploadTimeout = newGlobalMetadataUploadTimeout;
    }

    public TimeValue getGlobalMetadataUploadTimeout() {
        return this.globalMetadataUploadTimeout;
    }
}
