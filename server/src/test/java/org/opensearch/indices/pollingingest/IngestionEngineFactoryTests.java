/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.ingest.IngestService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class IngestionEngineFactoryTests extends OpenSearchTestCase {

    public void testConstructorWithIngestServiceSupplier() {
        IngestionConsumerFactory consumerFactory = mock(IngestionConsumerFactory.class);
        IngestService ingestService = mock(IngestService.class);

        IngestionEngineFactory factory = new IngestionEngineFactory(consumerFactory, () -> ingestService);
        assertNotNull(factory);
    }

    public void testConstructorWithoutIngestServiceSupplier() {
        IngestionConsumerFactory consumerFactory = mock(IngestionConsumerFactory.class);

        // Single-arg constructor for backward compatibility
        IngestionEngineFactory factory = new IngestionEngineFactory(consumerFactory);
        assertNotNull(factory);
    }

    public void testConstructorWithNullIngestServiceSupplier() {
        IngestionConsumerFactory consumerFactory = mock(IngestionConsumerFactory.class);

        // Null supplier should be accepted
        IngestionEngineFactory factory = new IngestionEngineFactory(consumerFactory, null);
        assertNotNull(factory);
    }

    public void testConstructorRejectsNullConsumerFactory() {
        expectThrows(NullPointerException.class, () -> new IngestionEngineFactory(null));
        expectThrows(NullPointerException.class, () -> new IngestionEngineFactory(null, () -> mock(IngestService.class)));
    }
}
