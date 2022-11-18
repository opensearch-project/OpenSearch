/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class JobDocVersionTests extends OpenSearchTestCase {

    public void testCompareTo() {
        // constructor parameters: primary_term, seqNo, version
        JobDocVersion version1 = new JobDocVersion(1L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(null) > 0);

        JobDocVersion version2 = new JobDocVersion(1L, 2L, 1L);
        Assert.assertTrue(version1.compareTo(version2) < 0);

        JobDocVersion version3 = new JobDocVersion(2L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(version3) < 0);
        Assert.assertTrue(version2.compareTo(version3) > 0);

        JobDocVersion version4 = new JobDocVersion(1L, 1L, 1L);
        Assert.assertTrue(version1.compareTo(version4) == 0);
    }
}
