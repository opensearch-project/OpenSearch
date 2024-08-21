/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class RemoteStoreNodeAttributeTests extends OpenSearchTestCase {

    static private final String KEY_ARN = "arn:aws:kms:us-east-1:123456789:key/6e9aa906-2cc3-4924-8ded-f385c78d9dcf";
    static private final String REGION = "us-east-1";

    public void testCryptoMetadata() throws UnknownHostException {
        String repoName = "remote-store-A";
        String repoTypeSettingKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, repoName);
        String repoCryptoMetadataKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoCryptoMetadataSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX, repoName);
        Map<String, String> attr = Map.of(
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            repoTypeSettingKey,
            "s3",
            repoSettingsKey,
            "abc",
            repoSettingsKey + "base_path",
            "xyz",
            repoCryptoMetadataKey + ".key_provider_name",
            "store-test",
            repoCryptoMetadataKey + ".key_provider_type",
            "aws-kms",
            repoCryptoMetadataSettingsKey + ".region",
            REGION,
            repoCryptoMetadataSettingsKey + ".key_arn",
            KEY_ARN
        );
        DiscoveryNode node = new DiscoveryNode(
            "C",
            new TransportAddress(InetAddress.getByName("localhost"), 9876),
            attr,
            emptySet(),
            Version.CURRENT
        );

        RemoteStoreNodeAttribute remoteStoreNodeAttribute = new RemoteStoreNodeAttribute(node);
        assertEquals(remoteStoreNodeAttribute.getRepositoriesMetadata().repositories().size(), 1);
        RepositoryMetadata repositoryMetadata = remoteStoreNodeAttribute.getRepositoriesMetadata().repositories().get(0);
        Settings.Builder settings = Settings.builder();
        settings.put("region", REGION);
        settings.put("key_arn", KEY_ARN);
        CryptoMetadata cryptoMetadata = new CryptoMetadata("store-test", "aws-kms", settings.build());
        assertEquals(cryptoMetadata, repositoryMetadata.cryptoMetadata());
    }

    public void testInvalidCryptoMetadata() throws UnknownHostException {
        String repoName = "remote-store-A";
        String repoTypeSettingKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, repoName);
        String repoCryptoMetadataKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoCryptoMetadataSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX, repoName);
        Map<String, String> attr = Map.of(
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            repoTypeSettingKey,
            "s3",
            repoSettingsKey,
            "abc",
            repoSettingsKey + "base_path",
            "xyz",
            repoCryptoMetadataSettingsKey + ".region",
            REGION,
            repoCryptoMetadataSettingsKey + ".key_arn",
            KEY_ARN
        );
        DiscoveryNode node = new DiscoveryNode(
            "C",
            new TransportAddress(InetAddress.getByName("localhost"), 9876),
            attr,
            emptySet(),
            Version.CURRENT
        );

        assertThrows(IllegalStateException.class, () -> new RemoteStoreNodeAttribute(node));
    }

    public void testNoCryptoMetadata() throws UnknownHostException {
        String repoName = "remote-store-A";
        String repoTypeSettingKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, repoName);
        Map<String, String> attr = Map.of(
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            repoTypeSettingKey,
            "s3",
            repoSettingsKey,
            "abc",
            repoSettingsKey + "base_path",
            "xyz"
        );
        DiscoveryNode node = new DiscoveryNode(
            "C",
            new TransportAddress(InetAddress.getByName("localhost"), 9876),
            attr,
            emptySet(),
            Version.CURRENT
        );

        RemoteStoreNodeAttribute remoteStoreNodeAttribute = new RemoteStoreNodeAttribute(node);
        assertEquals(remoteStoreNodeAttribute.getRepositoriesMetadata().repositories().size(), 1);
        RepositoryMetadata repositoryMetadata = remoteStoreNodeAttribute.getRepositoriesMetadata().repositories().get(0);
        assertNull(repositoryMetadata.cryptoMetadata());
    }

    public void testEqualsWithRepoSkip() throws UnknownHostException {
        String repoName = "remote-store-A";
        String repoTypeSettingKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, repoName);
        String repoSettingsKey = String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, repoName);
        Map<String, String> attr = Map.of(
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            repoTypeSettingKey,
            "s3",
            repoSettingsKey,
            "abc",
            repoSettingsKey + "base_path",
            "xyz"
        );
        DiscoveryNode node = new DiscoveryNode(
            "C",
            new TransportAddress(InetAddress.getByName("localhost"), 9876),
            attr,
            emptySet(),
            Version.CURRENT
        );

        RemoteStoreNodeAttribute remoteStoreNodeAttribute = new RemoteStoreNodeAttribute(node);

        String routingTableRepoName = "remote-store-B";
        String routingTableRepoTypeSettingKey = String.format(
            Locale.ROOT,
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            routingTableRepoName
        );
        String routingTableRepoSettingsKey = String.format(
            Locale.ROOT,
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            routingTableRepoName
        );

        Map<String, String> attr2 = Map.of(
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            repoName,
            REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            routingTableRepoName,
            repoTypeSettingKey,
            "s3",
            repoSettingsKey,
            "abc",
            repoSettingsKey + "base_path",
            "xyz",
            routingTableRepoTypeSettingKey,
            "s3",
            routingTableRepoSettingsKey,
            "xyz"
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "C",
            new TransportAddress(InetAddress.getByName("localhost"), 9876),
            attr2,
            emptySet(),
            Version.CURRENT
        );
        RemoteStoreNodeAttribute remoteStoreNodeAttribute2 = new RemoteStoreNodeAttribute(node2);

        assertFalse(remoteStoreNodeAttribute.equalsWithRepoSkip(remoteStoreNodeAttribute2, List.of()));
        assertTrue(remoteStoreNodeAttribute.equalsWithRepoSkip(remoteStoreNodeAttribute2, List.of(routingTableRepoName)));
    }
}
