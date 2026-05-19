/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Full-text search with window functions testing PPL integration test.
 */
public class FulltextWindowPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return FulltextWindowTestHelper.DATASET;
    }

    public void testFulltextWindowPplQueries() throws Exception {
        runPplQueries();
    }
}
