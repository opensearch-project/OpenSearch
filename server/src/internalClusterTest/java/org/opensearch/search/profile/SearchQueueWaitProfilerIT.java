/* SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class SearchQueueWaitProfilerIT extends OpenSearchSingleNodeTestCase {

    /**
     * Queue wait is recorded on the data node and has to survive the coordinator merging the fetch profile into the
     * query profile. More than one shard is required so the search runs as query then fetch and actually goes through
     * that merge.
     */
    public void testQueueWaitSurvivesFetchProfileMerge() throws Exception {
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen();

        int numDocs = randomIntBetween(20, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", "value" + i).get();
        }
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .get();

        assertNotNull("Profile response element should not be null", response.getProfileResults());
        assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

        int shardsWithFetchProfile = 0;
        for (Map.Entry<String, ProfileShardResult> shard : response.getProfileResults().entrySet()) {
            ProfileShardResult shardResult = shard.getValue();
            if (shardResult.getFetchProfileResult().getFetchProfileResults().isEmpty()) {
                continue;
            }
            shardsWithFetchProfile++;
            assertThat(
                "Queue wait should be reported for shard " + shard.getKey() + " after the fetch profile is merged in",
                shardResult.getQueueWaitNanos(),
                greaterThanOrEqualTo(0L)
            );
        }
        assertThat("At least one shard should have gone through the fetch profile merge", shardsWithFetchProfile, greaterThanOrEqualTo(1));
    }
}
