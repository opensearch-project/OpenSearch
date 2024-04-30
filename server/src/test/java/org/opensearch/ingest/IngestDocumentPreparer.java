/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import java.util.HashMap;
import java.util.Map;

public class IngestDocumentPreparer {
    public static final String SHOULD_FAIL_KEY = "shouldFail";

    public static IngestDocument createIngestDocument(boolean shouldFail) {
        Map<String, Object> source = new HashMap<>();
        if (shouldFail) {
            source.put(SHOULD_FAIL_KEY, true);
        }
        return new IngestDocument(source, new HashMap<>());
    }

    public static IngestDocumentWrapper createIngestDocumentWrapper(int slot) {
        return createIngestDocumentWrapper(slot, false);
    }

    public static IngestDocumentWrapper createIngestDocumentWrapper(int slot, boolean shouldFail) {
        return new IngestDocumentWrapper(slot, createIngestDocument(shouldFail), null);
    }
}
