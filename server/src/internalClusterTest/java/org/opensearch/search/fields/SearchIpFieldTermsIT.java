/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fields;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.Matchers.equalTo;

public class SearchIpFieldTermsIT extends OpenSearchSingleNodeTestCase {

    /**
     * @return number of expected matches
     * */
    private int createIndex(String indexName, int numberOfMasks, List<String> queryTermsSink) throws IOException {
        XContentBuilder xcb = createMapping();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).get();
        ensureGreen();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();

        Set<String> dedupeCidrs = new HashSet<>();
        int cidrs = 0;
        int ips = 0;

        for (int i = 0; ips <= 10240 && cidrs < numberOfMasks && i < 1000000; i++) {
            String ip;
            int prefix;
            boolean mask;
            do {
                mask = ips > 0 && random().nextBoolean();
                ip = generateRandomIPv4();
                prefix = 24 + random().nextInt(8); // CIDR prefix for IPv4
            } while (mask && !dedupeCidrs.add(getFirstThreeOctets(ip)));

            bulkRequestBuilder.add(
                client().prepareIndex(indexName).setSource(Map.of("addr", ip, "dummy_filter", randomSubsetOf(1, "1", "2", "3")))
            );

            final String termToQuery;
            if (mask) {
                termToQuery = ip + "/" + prefix;
                cidrs++;
            } else {
                termToQuery = ip;
                ips++;
            }
            queryTermsSink.add(termToQuery);
        }
        int addMatches = 0;
        for (int i = 0; i < atLeast(100); i++) {
            final String ip;
            ip = generateRandomIPv4();
            bulkRequestBuilder.add(
                client().prepareIndex(indexName).setSource(Map.of("addr", ip, "dummy_filter", randomSubsetOf(1, "1", "2", "3")))
            );
            boolean match = false;
            for (String termQ : queryTermsSink) {
                boolean isCidr = termQ.contains("/");
                if ((isCidr && isIPInCIDR(ip, termQ)) || (!isCidr && termQ.equals(ip))) {
                    match = true;
                    break;
                }
            }
            if (match) {
                addMatches++;
            } else {
                break; // single mismatch is enough.
            }
        }

        bulkRequestBuilder.setRefreshPolicy(IMMEDIATE).get();
        return ips + cidrs + addMatches;
    }

    public void testLessThanMaxClauses() throws IOException {
        ArrayList<String> toQuery = new ArrayList<>();
        String indexName = "small";
        int expectMatches = createIndex(indexName, IndexSearcher.getMaxClauseCount() - 1, toQuery);

        assertTermsHitCount(indexName, "addr", toQuery, expectMatches);
        assertTermsHitCount(indexName, "addr.idx", toQuery, expectMatches);
        assertTermsHitCount(indexName, "addr.dv", toQuery, expectMatches);
        // passing dummy filter crushes on rewriting
        SearchPhaseExecutionException ose = assertThrows(SearchPhaseExecutionException.class, () -> {
            assertTermsHitCount(
                indexName,
                "addr.dv",
                toQuery,
                expectMatches,
                (boolBuilder) -> boolBuilder.filter(QueryBuilders.termsQuery("dummy_filter", "1", "2", "3"))
                    .filter(QueryBuilders.termsQuery("dummy_filter", "1", "2", "3", "4"))
                    .filter(QueryBuilders.termsQuery("dummy_filter", "1", "2", "3", "4", "5"))
            );
        });
        assertTrue("exceeding on query rewrite", ose.shardFailures()[0].getCause() instanceof IndexSearcher.TooManyNestedClauses);
    }

    public void testExceedMaxClauses() throws IOException {
        ArrayList<String> toQuery = new ArrayList<>();
        String indexName = "larger";
        int expectMatches = createIndex(indexName, IndexSearcher.getMaxClauseCount() + (rarely() ? 0 : atLeast(10)), toQuery);
        assertTermsHitCount(indexName, "addr", toQuery, expectMatches);
        assertTermsHitCount(indexName, "addr.idx", toQuery, expectMatches);
        // error from mapper/parser
        final SearchPhaseExecutionException ose = assertThrows(
            SearchPhaseExecutionException.class,
            () -> assertTermsHitCount(indexName, "addr.dv", toQuery, expectMatches)
        );
        assertTrue("exceeding on query building", ose.shardFailures()[0].getCause().getCause() instanceof IndexSearcher.TooManyClauses);
    }

    private static String getFirstThreeOctets(String ipAddress) {
        // Split the IP address by the dot delimiter
        String[] octets = ipAddress.split("\\.");

        // Take the first three octets
        String[] firstThreeOctets = new String[3];
        System.arraycopy(octets, 0, firstThreeOctets, 0, 3);

        // Join the first three octets back together with dots
        return String.join(".", firstThreeOctets);
    }

    private void assertTermsHitCount(String indexName, String field, Collection<String> toQuery, long expectedMatches) {
        assertTermsHitCount(indexName, field, toQuery, expectedMatches, (bqb) -> {});
    }

    private void assertTermsHitCount(
        String indexName,
        String field,
        Collection<String> toQuery,
        long expectedMatches,
        Consumer<BoolQueryBuilder> addFilter
    ) {
        TermsQueryBuilder ipTerms = QueryBuilders.termsQuery(field, new ArrayList<>(toQuery));
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        addFilter.accept(boolQueryBuilder);
        SearchResponse result = client().prepareSearch(indexName).setQuery(boolQueryBuilder.must(ipTerms)
        // .filter(QueryBuilders.termsQuery("dummy_filter", "a", "b"))
        ).get();
        long hitsFound = Objects.requireNonNull(result.getHits().getTotalHits()).value;
        MatcherAssert.assertThat(field, hitsFound, equalTo(expectedMatches));
    }

    // Converts an IP string (either IPv4 or IPv6) to a byte array
    private static byte[] ipToBytes(String ip) {
        InetAddress inetAddress = InetAddresses.forString(ip);
        return inetAddress.getAddress();
    }

    // Checks if an IP is within a given CIDR (works for both IPv4 and IPv6)
    private static boolean isIPInCIDR(String ip, String cidr) {
        String[] cidrParts = cidr.split("/");
        String cidrIp = cidrParts[0];
        int prefixLength = Integer.parseInt(cidrParts[1]);

        byte[] ipBytes = ipToBytes(ip);
        byte[] cidrIpBytes = ipToBytes(cidrIp);

        // Calculate how many full bytes and how many bits are in the mask
        int fullBytes = prefixLength / 8;
        int extraBits = prefixLength % 8;

        // Compare full bytes
        for (int i = 0; i < fullBytes; i++) {
            if (ipBytes[i] != cidrIpBytes[i]) {
                return false;
            }
        }

        // Compare extra bits (if any)
        if (extraBits > 0) {
            int mask = 0xFF << (8 - extraBits);
            return (ipBytes[fullBytes] & mask) == (cidrIpBytes[fullBytes] & mask);
        }

        return true;
    }

    // Generate a random IPv4 address
    private String generateRandomIPv4() {
        return String.join(
            ".",
            String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256))
        );
    }

    private XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("addr")
            .field("type", "ip")
            .startObject("fields")
            .startObject("idx")
            .field("type", "ip")
            .field("doc_values", false)
            .endObject()
            .startObject("dv")
            .field("type", "ip")
            .field("index", false)
            .endObject()
            .endObject()
            .endObject()
            .startObject("dummy_filter")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
    }
}
