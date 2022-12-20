/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.Response;

/**
 * ActionListener for ExtensionsManager
 *
 * @opensearch.internal
 */
public class ExtensionActionListener implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(ExtensionActionListener.class);
    private List<Exception> exceptionList;

    public ExtensionActionListener() {
        exceptionList = new ArrayList<Exception>();
    }

    @Override
    public void onResponse(Response response) {
        logger.info("response {}", response);
    }

    @Override
    public void onFailure(Exception e) {
        exceptionList.add(e);
        logger.error(e.getMessage());
    }

    public List<Exception> getExceptionList() {
        return Collections.unmodifiableList(exceptionList);
    }
}
