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
import java.util.Map;

import static java.util.Collections.emptySet;

public class RemoteStoreNodeAttributeTests extends OpenSearchTestCase {

    static private final String KEY_ARN = "arn:aws:kms:us-east-1:123456789:key/6e9aa906-2cc3-4924-8ded-f385c78d9dcf";
    static private final String REGION = "us-east-1";

    public void testCryptoMetadata() throws UnknownHostException {
        Map<String, String> attr = Map.of(
            "remote_store.segment.repository",
            "remote-store-A",
            "remote_store.translog.repository",
            "remote-store-A",
            "remote_store.repository.remote-store-A.type",
            "s3",
            "remote_store.repository.remote-store-A.settings.bucket",
            "abc",
            "remote_store.repository.remote-store-A.settings.base_path",
            "xyz",
            "remote_store.repository.remote-store-A.crypto_metadata.key_provider_name",
            "store-test",
            "remote_store.repository.remote-store-A.crypto_metadata.key_provider_type",
            "aws-kms",
            "remote_store.repository.remote-store-A.crypto_metadata.settings.region",
            REGION,
            "remote_store.repository.remote-store-A.crypto_metadata.settings.key_arn",
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
        Map<String, String> attr = Map.of(
            "remote_store.segment.repository",
            "remote-store-A",
            "remote_store.translog.repository",
            "remote-store-A",
            "remote_store.repository.remote-store-A.type",
            "s3",
            "remote_store.repository.remote-store-A.settings.bucket",
            "abc",
            "remote_store.repository.remote-store-A.settings.base_path",
            "xyz",
            "remote_store.repository.remote-store-A.crypto_metadata.key_provider_name",
            "store-test",
            "remote_store.repository.remote-store-A.crypto_metadata.settings.region",
            REGION,
            "remote_store.repository.remote-store-A.crypto_metadata.settings.key_arn",
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
        Map<String, String> attr = Map.of(
            "remote_store.segment.repository",
            "remote-store-A",
            "remote_store.translog.repository",
            "remote-store-A",
            "remote_store.repository.remote-store-A.type",
            "s3",
            "remote_store.repository.remote-store-A.settings.bucket",
            "abc",
            "remote_store.repository.remote-store-A.settings.base_path",
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
}
