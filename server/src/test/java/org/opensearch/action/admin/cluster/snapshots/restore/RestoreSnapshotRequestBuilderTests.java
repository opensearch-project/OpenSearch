/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.restore;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

public class RestoreSnapshotRequestBuilderTests extends OpenSearchTestCase {

    private NoOpClient testClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.testClient = new NoOpClient(getTestName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.testClient.close();
        super.tearDown();
    }

    public void testSetAttachToDataStream() {
        RestoreSnapshotRequestBuilder builder = new RestoreSnapshotRequestBuilder(
            this.testClient,
            RestoreSnapshotAction.INSTANCE,
            "repo",
            "snap"
        );
        assertFalse(builder.request().attachToDataStream());

        RestoreSnapshotRequestBuilder returned = builder.setAttachToDataStream(true);
        // The setter is fluent and mutates the wrapped request.
        assertSame(builder, returned);
        assertTrue(builder.request().attachToDataStream());

        builder.setAttachToDataStream(false);
        assertFalse(builder.request().attachToDataStream());
    }
}
