/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Exception tracking and analysis PPL integration test.
 */
public class ExceptionLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return ExceptionLogsTestHelper.DATASET;
    }

    public void testExceptionLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
