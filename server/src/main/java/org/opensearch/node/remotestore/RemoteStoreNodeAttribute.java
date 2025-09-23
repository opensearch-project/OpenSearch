/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.node.Node;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
public class RemoteStoreNodeAttribute {

    private static final String REMOTE_STORE_TRANSLOG_REPO_PREFIX = "translog";
    private static final String REMOTE_STORE_SEGMENT_REPO_PREFIX = "segment";

    private static final String S3_ENCRYPTION_TYPE_KMS = "aws:kms";
    private static final String S3_SERVER_SIDE_ENCRYPTION_TYPE = "server_side_encryption_type";

    public static final List<String> REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = List.of("remote_store", "remote_publication");

    // TO-DO the string constants are used only for tests and can be moved to test package
    public static final String REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.state.repository";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.routing_table.repository";

    public static final List<String> REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".state.repository")
        .collect(Collectors.toList());

    public static final List<String> REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".routing_table.repository")
        .collect(Collectors.toList());
    public static final List<String> REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".segment.repository")
        .collect(Collectors.toList());
    public static final List<String> REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".translog.repository")
        .collect(Collectors.toList());

    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s."
        + CryptoMetadata.CRYPTO_METADATA_KEY;
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX = REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT
        + "."
        + CryptoMetadata.SETTINGS_KEY;
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";

    public static final String REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "%s.repository.%s.type";
    public static final String REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT = "%s.repository.%s." + CryptoMetadata.CRYPTO_METADATA_KEY;
    public static final String REPOSITORY_CRYPTO_SETTINGS_PREFIX = REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT
        + "."
        + CryptoMetadata.SETTINGS_KEY;
    public static final String REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "%s.repository.%s.settings.";

    public static final String REPOSITORY_SETTINGS_SSE_ENABLED_ATTRIBUTE_KEY = "sse_enabled";

    private final RepositoriesMetadata repositoriesMetadata;

    public static List<List<String>> SUPPORTED_DATA_REPO_NAME_ATTRIBUTES = Arrays.asList(
        REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS,
        REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS
    );

    public static final String REMOTE_STORE_MODE_KEY = "remote_store.mode";
    public static final String REMOTE_STORE_SSE_REPO_SUFFIX = "-sse";

    private static CompositeRemoteRepository compositeRemoteRepository;
    private static Map<String, RepositoryMetadata> repositoryMetadataMap;

    /**
     * Creates a new {@link RemoteStoreNodeAttribute}
     */
    public RemoteStoreNodeAttribute(DiscoveryNode node) {
        repositoryMetadataMap = new HashMap<>();
        compositeRemoteRepository = new CompositeRemoteRepository();
        this.repositoriesMetadata = buildRepositoriesMetadata(node);
    }

    private String getAndValidateNodeAttribute(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "]");
        }

        return attributeValue;
    }

    private Tuple<String, String> getAndValidateNodeAttributeEntries(DiscoveryNode node, List<String> attributeKeys) {
        Tuple<String, String> attributeValue = getValue(node.getAttributes(), attributeKeys);
        if (attributeValue == null || attributeValue.v1() == null || attributeValue.v1().isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKeys.get(0) + "]");
        }

        return attributeValue;
    }

    private CryptoMetadata buildCryptoMetadata(DiscoveryNode node, String repositoryName, String prefix) {
        String metadataKey = String.format(Locale.getDefault(), REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, prefix, repositoryName);
        boolean isRepoEncrypted = node.getAttributes().keySet().stream().anyMatch(key -> key.startsWith(metadataKey));
        if (isRepoEncrypted == false) {
            return null;
        }

        String keyProviderName = getAndValidateNodeAttribute(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_NAME_KEY);
        String keyProviderType = getAndValidateNodeAttribute(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_TYPE_KEY);

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REPOSITORY_CRYPTO_SETTINGS_PREFIX, prefix, repositoryName);

        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix + ".", ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        return new CryptoMetadata(keyProviderName, keyProviderType, settings.build());
    }

    private Map<String, String> getSettingAttribute(DiscoveryNode node, String repositoryName, String prefix) {
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            prefix,
            repositoryName
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> getAndValidateNodeAttribute(node, key)));

        if (settingsMap.isEmpty()) {
            throw new IllegalStateException(
                "joining node [" + node + "] doesn't have settings attribute for [" + repositoryName + "] repository"
            );
        }

        return settingsMap;
    }

    private List<RepositoryMetadata> buildRepositoryMetadata(DiscoveryNode node, String name, String prefix) {
        List<RepositoryMetadata> repoList = new ArrayList<>();
        String type = getAndValidateNodeAttribute(
            node,
            String.format(Locale.getDefault(), REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, prefix, name)
        );
        Map<String, String> settingsMap = getSettingAttribute(node, name, prefix);

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        CryptoMetadata cryptoMetadata = buildCryptoMetadata(node, name, prefix);

        // Repository metadata built here will always be for a system repository.
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);
        repoList.add(new RepositoryMetadata(name, type, settings.build(), cryptoMetadata));

        if (settingsMap.containsKey(REPOSITORY_SETTINGS_SSE_ENABLED_ATTRIBUTE_KEY)) {
            Settings.Builder sseSetting = Settings.builder();
            settingsMap.forEach(sseSetting::put);

            // Repository metadata built here will always be for a system repository.
            sseSetting.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);
            sseSetting.put(REPOSITORY_SETTINGS_SSE_ENABLED_ATTRIBUTE_KEY, true);
            sseSetting.put(S3_SERVER_SIDE_ENCRYPTION_TYPE, S3_ENCRYPTION_TYPE_KMS);
            repoList.add(new RepositoryMetadata(getServerSideEncryptedRepoName(name), type, sseSetting.build()));
        }
        return repoList;
    }

    private RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        Map<String, String> remoteStoryTypeToRepoNameMap = new HashMap<>();
        Map<String, String> repositoryNamesWithPrefix = getValidatedRepositoryNames(node, remoteStoryTypeToRepoNameMap);
        List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();

        for (Map.Entry<String, String> repository : repositoryNamesWithPrefix.entrySet()) {
            List<RepositoryMetadata> repoList = buildRepositoryMetadata(node, repository.getKey(), repository.getValue());
            RepositoryMetadata nonSSERepository = repoList.getFirst();

            repositoryMetadataList.add(nonSSERepository);
            repositoryMetadataMap.put(nonSSERepository.name(), nonSSERepository);

            if (repoList.size() > 1 && isCompositeRepository(repoList.getLast())) {
                RepositoryMetadata sseRepository = repoList.getLast();
                repositoryMetadataMap.put(sseRepository.name(), sseRepository);
                repositoryMetadataList.add(sseRepository);
            }
        }

        // Let's Iterate over repo's and build Composite Repository structure
        for (Map.Entry<String, String> repositoryTypeToNameEntry : remoteStoryTypeToRepoNameMap.entrySet()) {
            CompositeRemoteRepository.CompositeRepositoryEncryptionType encryptionType =
                CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT;
            CompositeRemoteRepository.RemoteStoreRepositoryType remoteStoreRepositoryType =
                CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT;
            if (repositoryTypeToNameEntry.getKey().contains(REMOTE_STORE_TRANSLOG_REPO_PREFIX)) {
                remoteStoreRepositoryType = CompositeRemoteRepository.RemoteStoreRepositoryType.TRANSLOG;
            }

            String repositoryName = repositoryTypeToNameEntry.getValue();
            compositeRemoteRepository.registerCompositeRepository(
                remoteStoreRepositoryType,
                encryptionType,
                repositoryMetadataMap.get(repositoryName)
            );

            String sseRepositoryName = getServerSideEncryptedRepoName(repositoryTypeToNameEntry.getValue());
            if (repositoryMetadataMap.containsKey(sseRepositoryName)) {
                encryptionType = CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER;
                compositeRemoteRepository.registerCompositeRepository(
                    remoteStoreRepositoryType,
                    encryptionType,
                    repositoryMetadataMap.get(sseRepositoryName)
                );
            }
        }
        return new RepositoriesMetadata(repositoryMetadataList);
    }

    private boolean isCompositeRepository(RepositoryMetadata repositoryMetadata) {
        return repositoryMetadata.settings().hasValue(REPOSITORY_SETTINGS_SSE_ENABLED_ATTRIBUTE_KEY);
    }

    private static Tuple<String, String> getValue(Map<String, String> attributes, List<String> keys) {
        for (String key : keys) {
            if (attributes.containsKey(key)) {
                return new Tuple<>(attributes.get(key), key);
            }
        }
        return null;
    }

    private enum RemoteStoreMode {
        SEGMENTS_ONLY,
        DEFAULT
    }

    private Map<String, String> getValidatedRepositoryNames(DiscoveryNode node, Map<String, String> remoteStoryTypeToRepoNameMap) {
        Set<Tuple<String, String>> repositoryNames = new HashSet<>();
        RemoteStoreMode remoteStoreMode = RemoteStoreMode.DEFAULT;
        if (containsKey(node.getAttributes(), List.of(REMOTE_STORE_MODE_KEY))) {
            String mode = node.getAttributes().get(REMOTE_STORE_MODE_KEY);
            if (mode != null && mode.equalsIgnoreCase(RemoteStoreMode.SEGMENTS_ONLY.name())) {
                remoteStoreMode = RemoteStoreMode.SEGMENTS_ONLY;
            } else if (mode != null && mode.equalsIgnoreCase(RemoteStoreMode.DEFAULT.name()) == false) {
                throw new IllegalStateException("Unknown remote store mode [" + mode + "] for node [" + node + "]");
            }
        }

        if (remoteStoreMode == RemoteStoreMode.SEGMENTS_ONLY) {
            addRepositoryNames(
                node,
                remoteStoryTypeToRepoNameMap,
                repositoryNames,
                REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS,
                REMOTE_STORE_SEGMENT_REPO_PREFIX
            );
        } else if (containsKey(node.getAttributes(), REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS)
            || containsKey(node.getAttributes(), REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
                addRepositoryNames(
                    node,
                    remoteStoryTypeToRepoNameMap,
                    repositoryNames,
                    REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS,
                    REMOTE_STORE_SEGMENT_REPO_PREFIX
                );
                addRepositoryNames(
                    node,
                    remoteStoryTypeToRepoNameMap,
                    repositoryNames,
                    REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS,
                    REMOTE_STORE_TRANSLOG_REPO_PREFIX
                );

                repositoryNames.add(getAndValidateNodeAttributeEntries(node, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
            } else if (containsKey(node.getAttributes(), REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
                repositoryNames.add(getAndValidateNodeAttributeEntries(node, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
            }
        if (containsKey(node.getAttributes(), REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
            if (remoteStoreMode == RemoteStoreMode.SEGMENTS_ONLY) {
                throw new IllegalStateException(
                    "Cannot set "
                        + REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS
                        + " attributes when remote store mode is set to segments only for node ["
                        + node
                        + "]"
                );
            }
            repositoryNames.add(getAndValidateNodeAttributeEntries(node, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
        }

        Map<String, String> repoNamesWithPrefix = new HashMap<>();
        repositoryNames.forEach(t -> {
            String[] attrKeyParts = t.v2().split("\\.");
            repoNamesWithPrefix.put(t.v1(), attrKeyParts[0]);
        });
        return repoNamesWithPrefix;
    }

    private void addRepositoryNames(
        DiscoveryNode node,
        Map<String, String> remoteStoryTypeToRepoNameMap,
        Set<Tuple<String, String>> repositoryNames,
        List<String> attributeKeys,
        String remoteStoreRepoPrefix
    ) {
        Tuple<String, String> remoteStoreAttributeKeyMap = getAndValidateNodeAttributeEntries(node, attributeKeys);
        remoteStoryTypeToRepoNameMap.put(remoteStoreRepoPrefix, remoteStoreAttributeKeyMap.v1());
        repositoryNames.add(remoteStoreAttributeKeyMap);
    }

    public static boolean isRemoteStoreAttributePresent(Settings settings) {
        for (String prefix : REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX) {
            if (settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + prefix).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRemoteDataAttributePresent(Settings settings) {
        return isSegmentRepoConfigured(settings) || isTranslogRepoConfigured(settings);
    }

    public static boolean isSegmentRepoConfigured(Settings settings) {
        for (String prefix : REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + prefix).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean isTranslogRepoConfigured(Settings settings) {
        for (String prefix : REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + prefix).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRemoteClusterStateConfigured(Settings settings) {
        for (String prefix : REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + prefix).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRemoteStoreServerSideEncryptionEnabled() {
        return compositeRemoteRepository != null && compositeRemoteRepository.isServerSideEncryptionEnabled();
    }

    public static boolean isRemoteStoreClusterStateEnabled(Settings settings) {
        return RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) && isRemoteClusterStateConfigured(settings);
    }

    private static boolean isRemoteRoutingTableAttributePresent(Settings settings) {
        for (String prefix : REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + prefix).isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean isRemoteRoutingTableConfigured(Settings settings) {
        return isRemoteRoutingTableAttributePresent(settings);
    }

    public RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    /**
     * Return {@link Map} of all the supported data repo names listed on {@link RemoteStoreNodeAttribute#SUPPORTED_DATA_REPO_NAME_ATTRIBUTES}
     *
     * @param node Node to fetch attributes from
     * @return {@link Map} of all remote store data repo attribute keys and their values
     */
    public static Map<String, String> getDataRepoNames(DiscoveryNode node) {
        assert remoteDataAttributesPresent(node.getAttributes());
        Map<String, String> dataRepoNames = new HashMap<>();
        for (List<String> supportedRepoAttribute : SUPPORTED_DATA_REPO_NAME_ATTRIBUTES) {
            Tuple<String, String> value = getValue(node.getAttributes(), supportedRepoAttribute);
            if (value != null && value.v1() != null) {
                dataRepoNames.put(value.v2(), value.v1());
            }
        }
        return dataRepoNames;
    }

    private static boolean remoteDataAttributesPresent(Map<String, String> nodeAttrs) {
        for (List<String> supportedRepoAttribute : SUPPORTED_DATA_REPO_NAME_ATTRIBUTES) {
            Tuple<String, String> value = getValue(nodeAttrs, supportedRepoAttribute);
            if (value == null || value.v1() == null) {
                return false;
            }
        }
        return true;
    }

    public static String getClusterStateRepoName(Map<String, String> repos) {
        return getValueFromAnyKey(repos, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static String getRoutingTableRepoName(Map<String, String> repos) {
        return getValueFromAnyKey(repos, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    private static String getRemoteStoreSegmentRepoFromNodeAttribute(Settings settings) {
        for (String prefix : REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.get(Node.NODE_ATTRIBUTES.getKey() + prefix) != null) {
                return settings.get(Node.NODE_ATTRIBUTES.getKey() + prefix);
            }
        }
        return null;
    }

    private static String getRemoteStoreTranslogRepoFromNodeAttribute(Settings settings) {
        for (String prefix : REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS) {
            if (settings.get(Node.NODE_ATTRIBUTES.getKey() + prefix) != null) {
                return settings.get(Node.NODE_ATTRIBUTES.getKey() + prefix);
            }
        }
        return null;
    }

    public static String getRemoteStoreSegmentRepo(boolean serverSideEncryptionEnabled) {
        if (compositeRemoteRepository == null) {
            return null;
        }

        CompositeRemoteRepository.RemoteStoreRepositoryType repositoryType = CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT;
        CompositeRemoteRepository.CompositeRepositoryEncryptionType encryptionType =
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT;
        if (serverSideEncryptionEnabled) {
            encryptionType = CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER;
        }
        RepositoryMetadata repositoryMetadata = compositeRemoteRepository.getRepository(repositoryType, encryptionType);
        return repositoryMetadata == null ? null : repositoryMetadata.name();
    }

    public static String getRemoteStoreSegmentRepo(Settings indexSettings) {
        // If already set in index setting no need to check composite repository.
        String segRepo = indexSettings.get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
        if (segRepo != null) {
            return segRepo;
        }
        return getRemoteStoreSegmentRepo(indexSettings.getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_SSE_ENABLED, false));
    }

    public static String getRemoteStoreTranslogRepo(boolean serverSideEncryptionEnabled) {
        if (compositeRemoteRepository == null) {
            return null;
        }

        CompositeRemoteRepository.RemoteStoreRepositoryType repositoryType = CompositeRemoteRepository.RemoteStoreRepositoryType.TRANSLOG;
        CompositeRemoteRepository.CompositeRepositoryEncryptionType encryptionType =
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT;
        if (serverSideEncryptionEnabled) {
            encryptionType = CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER;
        }
        RepositoryMetadata repositoryMetadata = compositeRemoteRepository.getRepository(repositoryType, encryptionType);
        return repositoryMetadata == null ? null : repositoryMetadata.name();
    }

    public static String getRemoteStoreTranslogRepo(Settings indexSettings) {
        String translogRepository = indexSettings.get(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY);
        if (translogRepository != null) {
            return translogRepository;
        }
        return getRemoteStoreTranslogRepo(indexSettings.getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_SSE_ENABLED, false));
    }

    private static String getValueFromAnyKey(Map<String, String> repos, List<String> keys) {
        for (String key : keys) {
            if (repos.get(key) != null) {
                return repos.get(key);
            }
        }
        return null;
    }

    public static String getClusterStateRepoName(Settings settings) {
        return getValueFromAnyKey(settings, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static String getRoutingTableRepoName(Settings settings) {
        return getValueFromAnyKey(settings, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    private static String getValueFromAnyKey(Settings settings, List<String> keys) {
        for (String key : keys) {
            if (settings.get(Node.NODE_ATTRIBUTES.getKey() + key) != null) {
                return settings.get(Node.NODE_ATTRIBUTES.getKey() + key);
            }
        }
        return null;
    }

    public static boolean isClusterStateRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean isRoutingTableRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean isSegmentRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static String getServerSideEncryptedRepoName(String remoteRepoName) {
        return remoteRepoName + REMOTE_STORE_SSE_REPO_SUFFIX;
    }

    private static boolean containsKey(Map<String, String> attributes, List<String> keys) {
        return keys.stream().filter(k -> attributes.containsKey(k)).findFirst().isPresent();
    }

    @Override
    public int hashCode() {
        // The hashCode is generated by computing the hash of all the repositoryMetadata present in
        // repositoriesMetadata without generation. Below is the modified list hashCode generation logic.

        int hashCode = 1;
        Iterator iterator = this.repositoriesMetadata.repositories().iterator();
        while (iterator.hasNext()) {
            RepositoryMetadata repositoryMetadata = (RepositoryMetadata) iterator.next();
            hashCode = 31 * hashCode + (repositoryMetadata == null
                ? 0
                : Objects.hash(repositoryMetadata.name(), repositoryMetadata.type(), repositoryMetadata.settings()));
        }
        return hashCode;
    }

    /**
     * Checks if 2 instances are equal, with option to skip check for a list of repos.
     * *
     * @param o other instance
     * @param reposToSkip list of repos to skip check for equality
     * @return {@code true} iff both instances are equal, not including the repositories in both instances if they are part of reposToSkip.
     */
    public boolean equalsWithRepoSkip(Object o, List<String> reposToSkip) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;
        return this.getRepositoriesMetadata().equalsIgnoreGenerationsWithRepoSkip(that.getRepositoriesMetadata(), reposToSkip);
    }

    public boolean equalsForRepositories(Object otherNode, List<String> repositoryToValidate) {
        if (this == otherNode) return true;
        if (otherNode == null || getClass() != otherNode.getClass()) return false;

        RemoteStoreNodeAttribute other = (RemoteStoreNodeAttribute) otherNode;
        return this.getRepositoriesMetadata().equalsIgnoreGenerationsForRepo(other.repositoriesMetadata, repositoryToValidate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{').append(this.repositoriesMetadata).append('}');
        return sb.toString();
    }
}
