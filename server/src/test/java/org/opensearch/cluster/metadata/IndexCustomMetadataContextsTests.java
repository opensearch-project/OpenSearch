/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.test.OpenSearchTestCase;

public class IndexCustomMetadataContextsTests extends OpenSearchTestCase {
    public void testFieldDomainContexts() {
        assertFalse(IndexCustomMetadataContexts.isVisible(IndexFieldDomainMetadata.CUSTOM_KEY, XContentContext.API));
        assertTrue(IndexCustomMetadataContexts.isVisible(IndexFieldDomainMetadata.CUSTOM_KEY, XContentContext.GATEWAY));
        assertTrue(IndexCustomMetadataContexts.isVisible(IndexFieldDomainMetadata.CUSTOM_KEY, XContentContext.SNAPSHOT));
    }

    public void testUnregisteredMetadataRemainsVisibleInEveryContext() {
        for (XContentContext context : XContentContext.values()) {
            assertTrue(IndexCustomMetadataContexts.isVisible("plugin_metadata_" + randomAlphaOfLength(10), context));
            assertTrue(IndexCustomMetadataContexts.isVisible(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, context));
        }
    }

    public void testRejectsMissingContext() {
        expectThrows(NullPointerException.class, () -> IndexCustomMetadataContexts.isVisible("plugin_metadata", null));
    }
}
