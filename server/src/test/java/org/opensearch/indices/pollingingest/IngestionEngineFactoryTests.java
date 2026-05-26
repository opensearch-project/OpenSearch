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

    public void testConstructorWithNullIngestServiceSupplier() {
        IngestionConsumerFactory consumerFactory = mock(IngestionConsumerFactory.class);

        expectThrows(NullPointerException.class, () -> new IngestionEngineFactory(consumerFactory, null));
    }
}
