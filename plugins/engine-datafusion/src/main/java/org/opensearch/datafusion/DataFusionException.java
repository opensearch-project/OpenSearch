/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.util.logging.Logger;

public class DataFusionException extends Throwable {

    private static Logger logger = Logger.getLogger(DataFusionException.class.getName());
    public DataFusionException(String errMsg) {
        logger.info("DataFusionException: " + errMsg);
    }
}
