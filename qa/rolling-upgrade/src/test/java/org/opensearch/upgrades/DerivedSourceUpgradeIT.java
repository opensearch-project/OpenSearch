/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Rolling-upgrade BWC for the derived-source multi-field store fix (TextFieldMapper: under
 * index.derived_source.enabled, text multi-fields/sub-fields are no longer force-stored, while the
 * parent text field still is). The forced store is a Lucene FieldType bit recomputed per node on
 * every mapping parse, so during the MIXED phase some shard copies (written by old nodes) store the
 * sub-field and others (new nodes) do not. This verifies _source reconstruction (GET) and search
 * (query) stay correct for documents indexed in every phase: OLD, MIXED and UPGRADED.
 *
 * NOTE: derived_source (index.derived_source.enabled) is available from 3.3.0 onwards (#18565).
 */
public class DerivedSourceUpgradeIT extends AbstractRollingTestCase {

    private static final String INDEX = "derived_src_mf_rolling";

    public void testDerivedSourceMultiFieldStoreRolling() throws Exception {
        assumeTrue(
            "derived_source (index.derived_source.enabled) is available from 3.3.0 onwards",
            UPGRADE_FROM_VERSION.onOrAfter(Version.fromString("3.3.0"))
        );
        switch (CLUSTER_TYPE) {
            case OLD:
                createDerivedSourceIndex();
                indexTitles(0, "quick brown fox");   // ids 0,1,2
                refreshIndex();
                // GET _source + query on old docs (old cluster)
                assertSourceTitle(0, "quick brown fox 0");
                assertMatchCount("title.sub", "fox", 3);
                assertMatchCount("title", "quick", 3);
                break;
            case MIXED:
                ensureHealth();
                // index mixed-phase docs once (MIXED runs once per node upgraded)
                if (firstMixedRound) {
                    indexTitles(100, "lazy dog");    // ids 100,101,102
                    refreshIndex();
                }
                // old docs survive; mixed docs (served across old+new nodes) are correct
                assertSourceTitle(0, "quick brown fox 0");
                assertMatchCount("title.sub", "fox", 3);
                assertSourceTitle(100, "lazy dog 100");
                assertMatchCount("title.sub", "dog", 3);
                break;
            case UPGRADED:
                ensureHealth();
                indexTitles(200, "sleepy cat");      // ids 200,201,202
                refreshIndex();
                // all three generations: GET _source + query
                assertSourceTitle(0, "quick brown fox 0");
                assertSourceTitle(100, "lazy dog 100");
                assertSourceTitle(200, "sleepy cat 200");
                assertMatchCount("title.sub", "fox", 3);
                assertMatchCount("title.sub", "dog", 3);
                assertMatchCount("title.sub", "cat", 3);
                assertMatchCount("title", "sleepy", 3);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createDerivedSourceIndex() throws IOException {
        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(
            "{"
                + "\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":1,\"index.derived_source.enabled\":true},"
                + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\",\"fields\":{\"sub\":{\"type\":\"text\"}}}}}"
                + "}"
        );
        client().performRequest(req);
    }

    private void indexTitles(int startId, String phrase) throws IOException {
        for (int i = 0; i < 3; i++) {
            int id = startId + i;
            Request doc = new Request("PUT", "/" + INDEX + "/_doc/" + id);
            doc.setJsonEntity("{\"title\":\"" + phrase + " " + id + "\"}");
            client().performRequest(doc);
        }
    }

    private void refreshIndex() throws IOException {
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    private void ensureHealth() throws IOException {
        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "90s");
        client().performRequest(health);
    }

    private void assertSourceTitle(int id, String expectedTitle) throws IOException {
        Map<String, Object> doc = entityAsMap(client().performRequest(new Request("GET", "/" + INDEX + "/_doc/" + id)));
        assertThat(XContentMapValues.extractValue("_source.title", doc), equalTo(expectedTitle));
    }

    private void assertMatchCount(String field, String term, int expectedHits) throws IOException {
        Request search = new Request("GET", "/" + INDEX + "/_search");
        search.setJsonEntity("{\"track_total_hits\":true,\"query\":{\"match\":{\"" + field + "\":\"" + term + "\"}}}");
        Map<String, Object> resp = entityAsMap(client().performRequest(search));
        int hits = ((Number) XContentMapValues.extractValue("hits.total.value", resp)).intValue();
        assertThat("hits for " + field + ":" + term, hits, equalTo(expectedHits));
    }
}
