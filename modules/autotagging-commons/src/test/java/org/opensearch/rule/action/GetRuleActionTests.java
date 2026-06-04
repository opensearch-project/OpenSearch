/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.OpenSearchTestCase;

public class GetRuleActionTests extends OpenSearchTestCase {
    public void testGetName() {
        assertEquals("cluster:admin/opensearch/rule/_get", GetRuleAction.NAME);
    }

    public void testGetResponseReader() {
        assertTrue(GetRuleAction.INSTANCE.getResponseReader() instanceof Writeable.Reader);
    }
}
