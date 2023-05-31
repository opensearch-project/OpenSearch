/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery.ec2;

import org.opensearch.test.OpenSearchTestCase;

public abstract class AbstractEc2DiscoveryTestCase extends OpenSearchTestCase implements ConfigPathSupport {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        setUpAwsProfile();
    }

    @Override
    public void tearDown() throws Exception {
        resetAwsProfile();
        super.tearDown();
    }
}
