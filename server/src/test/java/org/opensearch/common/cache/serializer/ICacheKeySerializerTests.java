/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.serializer;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class ICacheKeySerializerTests extends OpenSearchTestCase {
    // For these tests, we use BytesReference as K, since we already have a Serializer<BytesReference, byte[]> implementation
    public void testEquality() throws Exception {
        BytesReferenceSerializer keySer = new BytesReferenceSerializer();
        ICacheKeySerializer<BytesReference> serializer = new ICacheKeySerializer<>(keySer);

        int numDimensionsTested = 100;
        for (int i = 0; i < numDimensionsTested; i++) {
            CacheStatsDimension dim = getRandomDim();
            ICacheKey<BytesReference> key = new ICacheKey<>(getRandomBytesReference(), List.of(dim));
            byte[] serialized = serializer.serialize(key);
            assertTrue(serializer.equals(key, serialized));
            ICacheKey<BytesReference> deserialized = serializer.deserialize(serialized);
            assertEquals(key, deserialized);
            assertTrue(serializer.equals(deserialized, serialized));
        }
    }

    public void testDimNumbers() throws Exception {
        BytesReferenceSerializer keySer = new BytesReferenceSerializer();
        ICacheKeySerializer<BytesReference> serializer = new ICacheKeySerializer<>(keySer);

        for (int numDims : new int[] { 0, 5, 1000 }) {
            List<CacheStatsDimension> dims = new ArrayList<>();
            for (int j = 0; j < numDims; j++) {
                dims.add(getRandomDim());
            }
            ICacheKey<BytesReference> key = new ICacheKey<>(getRandomBytesReference(), dims);
            byte[] serialized = serializer.serialize(key);
            assertTrue(serializer.equals(key, serialized));
            ICacheKey<BytesReference> deserialized = serializer.deserialize(serialized);
            assertEquals(key, deserialized);
        }
    }

    public void testHashCodes() throws Exception {
        ICacheKey<String> key1 = new ICacheKey<>("key", List.of(new CacheStatsDimension("dimension_name", "dimension_value")));
        ICacheKey<String> key2 = new ICacheKey<>("key", List.of(new CacheStatsDimension("dimension_name", "dimension_value")));

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void testNullInputs() throws Exception {
        BytesReferenceSerializer keySer = new BytesReferenceSerializer();
        ICacheKeySerializer<BytesReference> serializer = new ICacheKeySerializer<>(keySer);

        assertNull(serializer.deserialize(null));
        ICacheKey<BytesReference> nullKey = new ICacheKey<>(null, List.of(getRandomDim()));
        assertNull(serializer.serialize(nullKey));
        assertNull(serializer.serialize(null));
        assertNull(serializer.serialize(new ICacheKey<>(getRandomBytesReference(), null)));
    }

    private CacheStatsDimension getRandomDim() {
        return new CacheStatsDimension(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private BytesReference getRandomBytesReference() {
        byte[] bytesValue = new byte[1000];
        Random rand = Randomness.get();
        rand.nextBytes(bytesValue);
        return new BytesArray(bytesValue);
    }
}
