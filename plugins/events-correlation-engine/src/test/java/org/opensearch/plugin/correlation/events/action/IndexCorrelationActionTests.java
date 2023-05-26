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

public class IndexCorrelationActionTests extends OpenSearchTestCase {

    public void testIndexCorrelationActionName() {
        Assert.assertNotNull(IndexCorrelationAction.INSTANCE.name());
        Assert.assertEquals(IndexCorrelationAction.INSTANCE.name(), IndexCorrelationAction.NAME);
    }
}
