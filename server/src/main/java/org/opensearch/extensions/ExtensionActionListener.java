/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.Response;

/**
 * ActionListener = for ExtensionsOrchestratore
 *
 * @opensearch.internal
 */
public class ExtensionActionListener<ExtensionBooleanResponse> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(ExtensionActionListener.class);
    private int successCount;
    private int failureCount;

    public ExtensionActionListener() {
        successCount = 0;
        failureCount = 0;
    }

    @Override
    public void onResponse(Response response) {
        logger.info("response {}", response);
        successCount++;
    }

    @Override
    public void onFailure(Exception e) {
        failureCount++;
    }

    public static Logger getLogger() {
        return logger;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public int getFailureCount() {
        return failureCount;
    }
}
