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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This is an extension of {@Code DiscoveryNode} which provides an abstraction for validating and storing information
 * specific to remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNode extends DiscoveryNode {

    public static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";
    private final DiscoveryNode node;
    private final RepositoriesMetadata repositoriesMetadata;

    /**
     * Creates a new {@link RemoteStoreNode}
     */
    public RemoteStoreNode(DiscoveryNode node) {
        super(node.getName(), node.getId(), node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
        this.node = node;
        this.repositoriesMetadata = buildRepositoriesMetadata(node);
    }

    private String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "].");
        }

        return attributeValue;
    }

    private Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String settingsAttributeKeyPrefix) {
        return node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(node, key)));
    }

    // TODO: Add logic to mark these repository as System Repository once thats merged.
    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = validateAttributeNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name)
        );
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name)
        );

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        return new RepositoryMetadata(name, type, settings.build());
    }

    private RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        String segmentRepositoryName = node.getAttributes().get(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
        String translogRepositoryName = node.getAttributes().get(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
        if (segmentRepositoryName.equals(translogRepositoryName)) {
            return new RepositoriesMetadata(Collections.singletonList(buildRepositoryMetadata(node, segmentRepositoryName)));
        } else {
            List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();
            repositoryMetadataList.add(buildRepositoryMetadata(node, segmentRepositoryName));
            repositoryMetadataList.add(buildRepositoryMetadata(node, translogRepositoryName));
            return new RepositoriesMetadata(repositoryMetadataList);
        }
    }

    RepositoriesMetadata getRepositoriesMetadata() {
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
