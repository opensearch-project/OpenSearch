/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore;

import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is an abstraction for validating and storing information specific to remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNode {

    public static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";
    private final RepositoriesMetadata repositoriesMetadata;
    private final DiscoveryNode node;

    /**
     * Creates a new {@link RemoteStoreNode}
     */
    public RemoteStoreNode(DiscoveryNode node) {
        this.node = node;
        this.repositoriesMetadata = buildRepositoriesMetadata();
    }

    private String validateAttributeNonNull(String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + this.node + "] doesn't have the node attribute [" + attributeKey + "]");
        }

        return attributeValue;
    }

    private Map<String, String> validateSettingsAttributesNonNull(String repositoryName) {
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repositoryName
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(key)));

        if (settingsMap.isEmpty()) {
            throw new IllegalStateException(
                "joining node [" + this.node + "] doesn't have settings attribute for [" + repositoryName + "] repository"
            );
        }

        return settingsMap;
    }

    private RepositoryMetadata buildRepositoryMetadata(String name) {
        String type = validateAttributeNonNull(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(name);

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        // Repository metadata built here will always be for a system repository.
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build());
    }

    private RepositoriesMetadata buildRepositoriesMetadata() {
        List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();
        Set<String> repositoryNames = new HashSet<>();

        repositoryNames.add(validateAttributeNonNull(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY));
        repositoryNames.add(validateAttributeNonNull(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY));

        for (String repositoryName : repositoryNames) {
            repositoryMetadataList.add(buildRepositoryMetadata(repositoryName));
        }

        return new RepositoriesMetadata(repositoryMetadataList);
    }

    public RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    @Override
    public int hashCode() {
        // We will hash the id and repositories metadata as its highly unlikely that two nodes will have same id and
        // repositories metadata but are actually different.
        return Objects.hash(node.getEphemeralId(), repositoriesMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNode that = (RemoteStoreNode) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{').append(this.node).append('}');
        sb.append('{').append(this.repositoriesMetadata).append('}');
        return super.toString();
    }
}
