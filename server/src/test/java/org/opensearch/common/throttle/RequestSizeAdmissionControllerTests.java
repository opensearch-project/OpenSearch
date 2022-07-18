/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.throttle;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.common.throttle.AdmissionController.Controllers.REQUEST_SIZE;

public class RequestSizeAdmissionControllerTests extends OpenSearchTestCase {

    private AdmissionControlSetting setting;
    private RequestSizeAdmissionController requestSizeController;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, Long> limits = Map.of("_search", 1000L, "_bulk", 1000L); // 1000 bytes
        setting = new AdmissionControlSetting(REQUEST_SIZE.value(), true, limits);
        requestSizeController = new RequestSizeAdmissionController(setting);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInitialState() {
        assertEquals(REQUEST_SIZE.value(), requestSizeController.getName());
        assertEquals(0, requestSizeController.getUsedQuota());
    }
}
