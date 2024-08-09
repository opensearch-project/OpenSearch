package org.opensearch.action.admin.indices.shards;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.admin.cluster.shards.CatShardsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.ClusterName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CatShardsResponseTests {

    private CatShardsResponse catShardsResponse;

    @Before
    public void setUp() {
        catShardsResponse = new CatShardsResponse();
    }

    @Test
    public void testGetAndSetClusterStateResponse() {
        assertNull(catShardsResponse.getClusterStateResponse());

        ClusterName clusterName = new ClusterName("1");
        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, null, false);
        catShardsResponse.setClusterStateResponse(clusterStateResponse);

        assertEquals(clusterStateResponse, catShardsResponse.getClusterStateResponse());
    }

    @Test
    public void testGetAndSetIndicesStatsResponse() {
        assertNull(catShardsResponse.getIndicesStatsResponse());

        final IndicesStatsResponse indicesStatsResponse = new IndicesStatsResponse(null, 0, 0, 0, null);
        catShardsResponse.setIndicesStatsResponse(indicesStatsResponse);

        assertEquals(indicesStatsResponse, catShardsResponse.getIndicesStatsResponse());
    }
}
