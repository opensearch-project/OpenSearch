/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fieldcaps;

import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.AbstractMultiClustersTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CrossClusterFieldCapabilitiesIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return Collections.singleton(REMOTE);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testReachableRemoteReportsNoFailure() {
        createIndices();

        FieldCapabilitiesResponse response = client().prepareFieldCaps("local_logs", REMOTE + ":logs*").setFields("*").get();

        assertEquals(Set.of("local_logs", REMOTE + ":logs"), Set.of(response.getIndices()));
        assertEquals(0, response.getFailures().size());
    }

    public void testUnreachableRemoteReportsTheExpressionsAskedOfIt() {
        createIndices();
        for (TransportService local : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
            for (TransportService remote : cluster(REMOTE).getInstances(TransportService.class)) {
                ((MockTransportService) local).addFailToSendNoConnectRule(remote);
            }
        }
        try {
            FieldCapabilitiesResponse response = client().prepareFieldCaps("local_logs", REMOTE + ":logs*")
                .setFields("*")
                .setIndexFilter(randomBoolean() ? QueryBuilders.matchAllQuery() : null)
                .get();

            assertEquals(Set.of("local_logs"), Set.of(response.getIndices()));
            assertEquals(Set.of(REMOTE + ":logs*"), response.getFailures().keySet());
        } finally {
            for (TransportService local : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
                ((MockTransportService) local).clearAllRules();
            }
        }
    }

    public void testMissingRemoteIndexIsNotReported() {
        createIndices();

        FieldCapabilitiesResponse response = client().prepareFieldCaps("local_logs", REMOTE + ":missing").setFields("*").get();

        assertEquals(Set.of("local_logs"), Set.of(response.getIndices()));
        assertEquals(0, response.getFailures().size());
    }

    private void createIndices() {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("local_logs").setMapping("timestamp", "type=date"));
        assertAcked(client(REMOTE).admin().indices().prepareCreate("logs").setMapping("timestamp", "type=date"));
    }
}
