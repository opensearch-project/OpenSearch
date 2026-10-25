/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg.credentials;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergClientCredentialsProviderTests extends OpenSearchTestCase {

    public void testRegisterAndCreateRoundTrip() {
        AwsCredentialsProvider original = StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk"));
        String key = IcebergClientCredentialsProvider.register(original);

        Map<String, String> props = new HashMap<>();
        props.put(IcebergClientCredentialsProvider.REGISTRY_KEY_PROPERTY, key);
        AwsCredentialsProvider resolved = IcebergClientCredentialsProvider.create(props);

        // Same credentials should come out the other side.
        assertEquals("ak", resolved.resolveCredentials().accessKeyId());
        assertEquals("sk", resolved.resolveCredentials().secretAccessKey());

        IcebergClientCredentialsProvider.deregister(key);
    }

    public void testMissingRegistryKeyThrows() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> IcebergClientCredentialsProvider.create(Collections.emptyMap())
        );
        assertTrue(e.getMessage().contains(IcebergClientCredentialsProvider.REGISTRY_KEY_PROPERTY));
    }

    public void testUnknownRegistryKeyThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergClientCredentialsProvider.REGISTRY_KEY_PROPERTY, "no-such-key");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> IcebergClientCredentialsProvider.create(props));
        assertTrue(e.getMessage().contains("no credentials provider"));
    }

    public void testDeregisterIsIdempotent() {
        AwsCredentialsProvider original = StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk"));
        String key = IcebergClientCredentialsProvider.register(original);
        IcebergClientCredentialsProvider.deregister(key);
        IcebergClientCredentialsProvider.deregister(key);
        IcebergClientCredentialsProvider.deregister(null);
    }
}
