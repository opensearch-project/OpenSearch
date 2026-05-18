/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Complex Regex PPL integration test. Runs PPL queries against complex_regex data.
 */
public class ComplexRegexPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return ComplexRegexTestHelper.DATASET;
    }

    public void testComplexRegexPplQueries() throws Exception {
        runPplQueries();
    }
}
