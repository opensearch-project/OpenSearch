/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Complex Redesigned (multi-index) PPL integration test. Runs PPL queries against complex_redesigned data.
 */
public class MultiSourceJoinsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return MultiSourceJoinsTestHelper.DATASET;
    }

    public void testMultiSourceJoinsPplQueries() throws Exception {
        runPplQueries();
    }
}
