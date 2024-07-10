/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.DiffableUtils.NonDiffableValueSerializer;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.AbstractRemoteWritableEntityManager;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteGlobalMetadata;
import org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

/**
 * A Manager which provides APIs to write and read Global Metadata attributes to remote store
 *
 * @opensearch.internal
 */
public class RemoteGlobalMetadataManager extends AbstractRemoteWritableEntityManager {

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 1;

    private volatile TimeValue globalMetadataUploadTimeout;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;

    RemoteGlobalMetadataManager(
        ClusterSettings clusterSettings,
        String clusterName,
        BlobStoreRepository blobStoreRepository,
        BlobStoreTransferService blobStoreTransferService,
        NamedWriteableRegistry namedWriteableRegistry,
        ThreadPool threadpool
    ) {
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.compressor = blobStoreRepository.getCompressor();
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.remoteWritableEntityStores.put(
            RemoteGlobalMetadata.GLOBAL_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemoteCoordinationMetadata.COORDINATION_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemotePersistentSettingsMetadata.SETTING_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemoteTemplatesMetadata.TEMPLATES_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.remoteWritableEntityStores.put(
            RemoteCustomMetadata.CUSTOM_METADATA,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
    }

    @Override
    protected ActionListener<Void> getWriteActionListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteObject,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    ) {
        return ActionListener.wrap(
            resp -> listener.onResponse(remoteObject.getUploadedMetadata()),
            ex -> listener.onFailure(new RemoteStateTransferException("Upload failed for " + component, remoteObject, ex))
        );
    }

    @Override
    protected ActionListener<Object> getReadActionListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteObject,
        ActionListener<RemoteReadResult> listener
    ) {
        return ActionListener.wrap(
            response -> listener.onResponse(new RemoteReadResult(response, remoteObject.getType(), component)),
            ex -> listener.onFailure(new RemoteStateTransferException("Download failed for " + component, remoteObject, ex))
        );
    }

    Metadata getGlobalMetadata(String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        String globalMetadataFileName = clusterMetadataManifest.getGlobalMetadataFileName();
        try {
            // Fetch Global metadata
            if (globalMetadataFileName != null) {
                RemoteGlobalMetadata remoteGlobalMetadata = new RemoteGlobalMetadata(
                    String.format(Locale.ROOT, METADATA_NAME_FORMAT, globalMetadataFileName),
                    clusterUUID,
                    compressor,
                    namedXContentRegistry
                );
                return (Metadata) getStore(remoteGlobalMetadata).read(remoteGlobalMetadata);
            } else if (clusterMetadataManifest.hasMetadataAttributesFiles()) {
                // from CODEC_V2, we have started uploading all the metadata in granular files instead of a single entity
                Metadata.Builder builder = new Metadata.Builder();
                if (clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename() != null) {
                    RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(
                        clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.coordinationMetadata(
                        (CoordinationMetadata) getStore(remoteCoordinationMetadata).read(remoteCoordinationMetadata)
                    );
                }
                if (clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename() != null) {
                    RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata(
                        clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.templates((TemplatesMetadata) getStore(remoteTemplatesMetadata).read(remoteTemplatesMetadata));
                }
                if (clusterMetadataManifest.getSettingsMetadata().getUploadedFilename() != null) {
                    RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata(
                        clusterMetadataManifest.getSettingsMetadata().getUploadedFilename(),
                        clusterUUID,
                        compressor,
                        namedXContentRegistry
                    );
                    builder.persistentSettings(
                        (Settings) getStore(remotePersistentSettingsMetadata).read(remotePersistentSettingsMetadata)
                    );
                }
                builder.clusterUUID(clusterMetadataManifest.getClusterUUID());
                builder.clusterUUIDCommitted(clusterMetadataManifest.isClusterUUIDCommitted());
                clusterMetadataManifest.getCustomMetadataMap().forEach((key, value) -> {
                    try {
                        RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata(
                            value.getUploadedFilename(),
                            key,
                            clusterUUID,
                            compressor,
                            namedWriteableRegistry
                        );
                        builder.putCustom(key, (Custom) getStore(remoteCustomMetadata).read(remoteCustomMetadata));
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

    DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> getCustomsDiff(
        ClusterState currentState,
        ClusterState previousState,
        boolean firstUploadForSplitGlobalMetadata,
        boolean isRemotePublicationEnabled
    ) {
        if (firstUploadForSplitGlobalMetadata) {
            // For first split global metadata upload, we want to upload all customs
            return DiffableUtils.diff(
                Collections.emptyMap(),
                filterCustoms(currentState.metadata().customs(), isRemotePublicationEnabled),
                DiffableUtils.getStringKeySerializer(),
                NonDiffableValueSerializer.getAbstractInstance()
            );
        }
        return DiffableUtils.diff(
            filterCustoms(previousState.metadata().customs(), isRemotePublicationEnabled),
            filterCustoms(currentState.metadata().customs(), isRemotePublicationEnabled),
            DiffableUtils.getStringKeySerializer(),
            NonDiffableValueSerializer.getAbstractInstance()
        );
    }

    public static Map<String, Metadata.Custom> filterCustoms(Map<String, Metadata.Custom> customs, boolean isRemotePublicationEnabled) {
        if (isRemotePublicationEnabled) {
            return customs;
        }
        return customs.entrySet()
            .stream()
            .filter(e -> e.getValue().context().contains(XContentContext.GATEWAY))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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
