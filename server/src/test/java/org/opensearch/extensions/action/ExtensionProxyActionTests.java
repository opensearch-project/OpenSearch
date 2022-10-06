/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.test.OpenSearchTestCase;

public class ExtensionProxyActionTests extends OpenSearchTestCase {
    public void testExtensionProxyAction() {
        assertEquals("cluster:internal/extensions", ExtensionProxyAction.NAME);
        assertEquals(ExtensionProxyAction.class, ExtensionProxyAction.INSTANCE.getClass());
    }
}
