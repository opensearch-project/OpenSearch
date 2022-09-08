/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionFuture;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.ExecutionException;

public class ExtensionsActionTests extends OpenSearchSingleNodeTestCase {

    public void testExtensionAction() throws ExecutionException, InterruptedException {
        ExtensionsActionRequest request = new ExtensionsActionRequest("MyFirstTransportRequest");
        ActionFuture<ExtensionsActionResponse> execute = client().execute(ExtensionsAction.INSTANCE, request);
    }
}
