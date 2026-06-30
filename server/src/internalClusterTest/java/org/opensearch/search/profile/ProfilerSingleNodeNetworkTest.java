/* SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.apache.lucene.tests.util.English;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.profile.query.RandomQueryGenerator.randomQueryBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ProfilerSingleNodeNetworkTest extends OpenSearchSingleNodeTestCase {

    /**
     * This test checks to make sure in a single node cluster, the network time
     * is 0 as expected in the profiler for inbound an doutbound network time.
     */
    public void testProfilerNetworkTime() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        int iters = between(20, 100);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
            logger.info("Query: {}", q);

            SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setTrackTotalHits(true)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .get();

            assertNotNull("Profile response element should not be null", resp.getProfileResults());
            assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));
            for (Map.Entry<String, ProfileShardResult> shard : resp.getProfileResults().entrySet()) {
                assertThat(
                    "Profile response inbound network time should be 0 in single node clusters",
                    shard.getValue().getNetworkTime().getInboundNetworkTime(),
                    is(0L)
                );
                assertThat(
                    "Profile response outbound network time should be 0 in single node clusters",
                    shard.getValue().getNetworkTime().getOutboundNetworkTime(),
                    is(0L)
                );
            }
        }
    }
}
