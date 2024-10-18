/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.Matchers.equalTo;

public class SearchIpFieldTermsTest extends OpenSearchSingleNodeTestCase {

    public static final boolean IPv4_ONLY = true;
    static String defaultIndexName = "test";

    public void testMassive() throws Exception {
        XContentBuilder xcb = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(xcb).get();
        ensureGreen();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();

        int cidrs = 0;
        int ips = 0;
        List<String> toQuery = new ArrayList<>();
        for (int i = 0; ips <= 1024 && i < 1000000; i++) {
            final String ip;
            final int prefix;
            if (IPv4_ONLY) {
                ip = generateRandomIPv4();
                prefix = 8 + random().nextInt(24); // CIDR prefix for IPv4
            } else {
                ip = generateRandomIPv6();
                prefix = 32 + random().nextInt(97); // CIDR prefix for IPv6
            }

            bulkRequestBuilder.add(client().prepareIndex(defaultIndexName).setSource(Map.of("addr", ip)));

            final String termToQuery;
            if (cidrs < 1024 - 1 && random().nextBoolean()) {
                termToQuery = ip + "/" + prefix;
                cidrs++;
            } else {
                termToQuery = ip;
                ips++;
            }
            toQuery.add(termToQuery);
        }
        int addMatches = 0;
        for (int i = 0; i < atLeast(100); i++) {
            final String ip;
            if (IPv4_ONLY) {
                ip = generateRandomIPv4();
            } else {
                ip = generateRandomIPv6();
            }
            bulkRequestBuilder.add(client().prepareIndex(defaultIndexName).setSource(Map.of("addr", ip)));
            boolean match = false;
            for (String termQ : toQuery) {
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
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.termsQuery("addr", toQuery)).get();
        MatcherAssert.assertThat(Objects.requireNonNull(result.getHits().getTotalHits()).value, equalTo((long) cidrs + ips + addMatches));
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
        return String.join(".", String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256)),
            String.valueOf(random().nextInt(256)));
    }

    // Generate a random IPv6 address
    private static String generateRandomIPv6() {
        StringBuilder ipv6 = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            ipv6.append(Integer.toHexString(random().nextInt(0xFFFF + 1)));
            if (i < 7) {
                ipv6.append(":");
            }
        }
        return ipv6.toString();
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
            .endObject()
            .endObject();
    }
}
