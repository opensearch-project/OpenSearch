/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.core.index.Index;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DocumentLookupProviderTests extends OpenSearchTestCase {

    private static final Index INDEX = new Index("idx", "uuid");

    /** A provider that overrides only the abstract getById; getVersionMetadata/getDocsAboveSeqNo use the SPI defaults. */
    private static final DocumentLookupProvider DEFAULTS = (get, reader, index, resolver) -> DocumentLookupResult.notFound("x");

    public void testGetVersionMetadataDefaultsToNotFound() throws IOException {
        DocumentLookupResult result = DEFAULTS.getVersionMetadata("id", null, INDEX, DocumentMetadataResolver.NOOP);
        assertFalse(result.exists());
        assertEquals("id", result.id());
    }

    public void testGetDocsAboveSeqNoDefaultsToEmpty() throws IOException {
        assertTrue(DEFAULTS.getDocsAboveSeqNo(0L, null, INDEX, DocumentMetadataResolver.NOOP).isEmpty());
    }
}
