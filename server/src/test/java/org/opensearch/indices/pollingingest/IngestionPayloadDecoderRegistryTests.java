/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionPayloadDecoderFactory;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IngestionPayloadDecoderRegistryTests extends OpenSearchTestCase {

    public void testRegisterAndGet() {
        IngestionPayloadDecoderFactory factory = mock(IngestionPayloadDecoderFactory.class);
        IngestionPayloadDecoderRegistry registry = IngestionPayloadDecoderRegistry.builder().register("xcontent", factory).build();

        assertSame(factory, registry.get("xcontent"));
    }

    public void testDuplicateRegistrationThrows() {
        IngestionPayloadDecoderFactory factoryA = mock(IngestionPayloadDecoderFactory.class);
        IngestionPayloadDecoderFactory factoryB = mock(IngestionPayloadDecoderFactory.class);
        IngestionPayloadDecoderRegistry.Builder builder = IngestionPayloadDecoderRegistry.builder().register("xcontent", factoryA);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.register("xcontent", factoryB));
        assertTrue(e.getMessage().contains("xcontent"));
    }

    public void testGetUnknownTypeThrows() {
        IngestionPayloadDecoderRegistry registry = IngestionPayloadDecoderRegistry.builder()
            .register("xcontent", mock(IngestionPayloadDecoderFactory.class))
            .build();

        expectThrows(IllegalArgumentException.class, () -> registry.get("unknown"));
    }

    public void testCloseClosesAllRegisteredFactories() throws Exception {
        IngestionPayloadDecoderFactory factoryA = mock(IngestionPayloadDecoderFactory.class);
        IngestionPayloadDecoderFactory factoryB = mock(IngestionPayloadDecoderFactory.class);
        IngestionPayloadDecoderRegistry registry = IngestionPayloadDecoderRegistry.builder()
            .register("a", factoryA)
            .register("b", factoryB)
            .build();

        registry.close();

        verify(factoryA).close();
        verify(factoryB).close();
    }
}
