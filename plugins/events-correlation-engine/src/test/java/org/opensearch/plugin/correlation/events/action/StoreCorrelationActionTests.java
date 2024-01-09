/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class StoreCorrelationActionTests extends OpenSearchTestCase {

    public void testStoreCorrelationActionName() {
        Assert.assertNotNull(StoreCorrelationAction.INSTANCE.name());
        Assert.assertEquals(StoreCorrelationAction.INSTANCE.name(), StoreCorrelationAction.NAME);
    }
}
