/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.logging;

import org.apache.lucene.util.SetOnce;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeAndClusterIdConverterTest {

    public static final String NODE_ID = "node-id";
    public static final String CLUSTER_UUID = "cluster-uuid";

    @After
    @Before
    public void cleanSystemProperties() {
        System.clearProperty(NodeAndClusterIdConverter.ENABLED_SYSTEM_PROPERTY);
    }

    @Test(expected = SetOnce.AlreadySetException.class)
    public void testTryToSetClusterAndNodeIdByDefault() {
        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);
        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);
    }

    @Test(expected = SetOnce.AlreadySetException.class)
    public void testTryToSetClusterAndNodeIdWhenPluginIsEnabled() {
        System.setProperty(NodeAndClusterIdConverter.ENABLED_SYSTEM_PROPERTY, "true");

        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);
        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);
    }

    @Test
    public void testNotSetClusterIdWhenPluginIsDisabled() {
        System.setProperty(NodeAndClusterIdConverter.ENABLED_SYSTEM_PROPERTY, "false");

        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);

        // second invocation should not throw an exception
        NodeAndClusterIdConverter.setNodeIdAndClusterId(NODE_ID, CLUSTER_UUID);
    }

}
