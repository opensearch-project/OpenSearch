/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

public class FipsTrustStoreTests extends OpenSearchTestCase {

    public void testParse() {
        assertEquals(FipsTrustStore.TrustStoreSource.PREDEFINED, FipsTrustStore.TrustStoreSource.parse("PREDEFINED"));
        assertEquals(FipsTrustStore.TrustStoreSource.SYSTEM_TRUST, FipsTrustStore.TrustStoreSource.parse("SYSTEM_TRUST"));
        assertEquals(FipsTrustStore.TrustStoreSource.GENERATED, FipsTrustStore.TrustStoreSource.parse("GENERATED"));

        assertEquals(FipsTrustStore.TrustStoreSource.PREDEFINED, FipsTrustStore.TrustStoreSource.parse(" predefined"));
        assertEquals(FipsTrustStore.TrustStoreSource.PREDEFINED, FipsTrustStore.TrustStoreSource.parse(""));
        assertEquals(FipsTrustStore.TrustStoreSource.PREDEFINED, FipsTrustStore.TrustStoreSource.parse(null));

        var exception = assertThrows(IllegalArgumentException.class, () -> FipsTrustStore.TrustStoreSource.parse("INVALID"));
        assertEquals("Illegal truststore.source value [INVALID]", exception.getMessage());
    }

    public void testToString() {
        assertEquals("predefined", FipsTrustStore.TrustStoreSource.PREDEFINED.toString());
        assertEquals("system_trust", FipsTrustStore.TrustStoreSource.SYSTEM_TRUST.toString());
        assertEquals("generated", FipsTrustStore.TrustStoreSource.GENERATED.toString());
    }

    public void testSettingObject() {
        assertNotNull(FipsTrustStore.CLUSTER_FIPS_TRUSTSTORE_SOURCE_SETTING);
    }
}
