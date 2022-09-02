/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.util.ArrayList;

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
    private int responseCount;
    private ArrayList<Exception> exceptionList;

    public ExtensionActionListener() {
        responseCount = 0;
        exceptionList = new ArrayList<Exception>();
    }

    @Override
    public void onResponse(Response response) {
        logger.info("response {}", response);
        responseCount++;
    }

    @Override
    public void onFailure(Exception e) {
        responseCount++;
        exceptionList.add(e);
        logger.error(e.getMessage());
    }

    public static Logger getLogger() {
        return logger;
    }

    public int getResponseCount() {
        return responseCount;
    }

    public ArrayList<Exception> getExceptionList() {
        return exceptionList;
    }
}
