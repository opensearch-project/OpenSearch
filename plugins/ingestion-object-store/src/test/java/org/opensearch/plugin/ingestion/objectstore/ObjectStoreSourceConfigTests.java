/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link ObjectStoreSourceConfig} parsing of string, number, and defaulted parameters.
 */
public class ObjectStoreSourceConfigTests extends OpenSearchTestCase {

    private static Map<String, Object> base() {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "equal_bytes");
        return params;
    }

    public void testNullParamsThrows() {
        expectThrows(NullPointerException.class, () -> new ObjectStoreSourceConfig(null));
    }

    public void testFractionalNumericParamRejected() {
        Map<String, Object> params = base();
        params.put("max_record_bytes", 10.9);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreSourceConfig(params));
        assertThat(e.getMessage(), containsString("must be an integer"));
    }

    public void testHugeDoubleParamRejectedInsteadOfBecomingUnlimited() {
        Map<String, Object> params = base();
        params.put("max_bytes", 1e20);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreSourceConfig(params));
        assertThat(e.getMessage(), containsString("outside the range of a long"));
    }

    public void testInfiniteParamRejected() {
        Map<String, Object> params = base();
        params.put("max_bytes", Double.POSITIVE_INFINITY);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreSourceConfig(params));
        assertThat(e.getMessage(), containsString("must be an integer"));
    }

    public void testOutOfRangeBigIntegerParamRejected() {
        Map<String, Object> params = base();
        params.put("max_get_requests", new java.math.BigInteger("18446744073709551616"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreSourceConfig(params));
        assertThat(e.getMessage(), containsString("outside the range of a long"));
    }

    public void testLongMaxValueStillAccepted() {
        Map<String, Object> params = base();
        params.put("max_bytes", Long.MAX_VALUE);
        assertThat(new ObjectStoreSourceConfig(params).getMaxBytes(), is(Long.MAX_VALUE));
    }

    public void testDefaultsApplied() {
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(base());
        assertThat(config.getFormat(), equalTo(ObjectStoreSourceConfig.DEFAULT_FORMAT));
        assertThat(config.getMaxRecordBytes(), is(ObjectStoreSourceConfig.DEFAULT_MAX_RECORD_BYTES));
        assertThat(config.getMaxObjects(), is(ObjectStoreSourceConfig.DEFAULT_MAX_OBJECTS));
        assertThat(config.getMaxListRequests(), is(Long.MAX_VALUE));
        assertThat(config.getMaxGetRequests(), is(Long.MAX_VALUE));
        assertThat(config.getMaxBytes(), is(Long.MAX_VALUE));
        assertThat(config.getSourceUrl(), is(nullValue()));
        assertThat(config.getSourcePrefix(), is(nullValue()));
        assertThat(config.getManifestJson(), is(nullValue()));
    }

    public void testManifestJsonGetterReturnsRawParam() {
        Map<String, Object> params = base();
        String manifest = "[{\"key\":\"a.ndjson\",\"size\":10}]";
        params.put("manifest", manifest);
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getManifestJson(), equalTo(manifest));
    }

    public void testAllGetters() {
        Map<String, Object> params = base();
        params.put("source_url", "dump/a.ndjson");
        params.put("format", "ndjson");
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getRepository(), equalTo("repo"));
        assertThat(config.getSourceUrl(), equalTo("dump/a.ndjson"));
        assertThat(config.getShardSplit(), is(ShardSplit.EQUAL_BYTES));
    }

    public void testReadLongFromNumber() {
        Map<String, Object> params = base();
        params.put("max_record_bytes", 4096);
        params.put("max_objects", 12L);
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getMaxRecordBytes(), is(4096L));
        assertThat(config.getMaxObjects(), is(12L));
    }

    public void testReadLongFromNumericString() {
        Map<String, Object> params = base();
        params.put("max_objects", "500");
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getMaxObjects(), is(500L));
    }

    public void testReadLongNonNumericThrows() {
        Map<String, Object> params = base();
        params.put("max_record_bytes", "not-a-number");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreSourceConfig(params));
        assertThat(e.getMessage(), containsString("must be a long"));
    }

    public void testCostCapsFromNumbers() {
        Map<String, Object> params = base();
        params.put("max_list_requests", 5);
        params.put("max_get_requests", 7L);
        params.put("max_bytes", 1024);
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getMaxListRequests(), is(5L));
        assertThat(config.getMaxGetRequests(), is(7L));
        assertThat(config.getMaxBytes(), is(1024L));
    }

    public void testCostCapsFromNumericStrings() {
        Map<String, Object> params = base();
        params.put("max_list_requests", "3");
        params.put("max_get_requests", "9");
        params.put("max_bytes", "2048");
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(params);
        assertThat(config.getMaxListRequests(), is(3L));
        assertThat(config.getMaxGetRequests(), is(9L));
        assertThat(config.getMaxBytes(), is(2048L));
    }

    public void testDiscoveryDefaultsToManifest() {
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(Map.of("repository", "repo", "shard_split", "whole_files"));
        assertThat(config.getDiscovery(), is(Discovery.MANIFEST));
        assertThat(ObjectStoreSourceConfig.DEFAULT_DISCOVERY, is(Discovery.MANIFEST));
    }

    public void testDiscoveryReadFromParams() {
        ObjectStoreSourceConfig config = new ObjectStoreSourceConfig(
            Map.of("repository", "repo", "shard_split", "shard_prefixes", "discovery", "list")
        );
        assertThat(config.getDiscovery(), is(Discovery.LIST));
    }

    public void testUnknownDiscoveryRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ObjectStoreSourceConfig(Map.of("repository", "repo", "shard_split", "whole_files", "discovery", "auto"))
        );
    }
}
