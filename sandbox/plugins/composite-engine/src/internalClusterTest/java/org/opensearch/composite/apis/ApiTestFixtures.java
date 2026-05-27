/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.apis;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.Client;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Fixture creator helpers for composite engine API integration tests.
 * Provides static methods that index documents of various shapes for testing.
 *
 * @opensearch.experimental
 */
public class ApiTestFixtures {

    private ApiTestFixtures() {}

    /**
     * Indexes typed docs across multiple field types:
     * kw (keyword), int_v (integer), long_v (long), dbl_v (double), bool_v (boolean), date_v (date)
     */
    public static void indexMultiTypeDocs(Client client, String index, int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client.prepareIndex()
                    .setIndex(index)
                    .setId(String.valueOf(i))
                    .setSource(
                        "kw",
                        "keyword_" + i,
                        "int_v",
                        i,
                        "long_v",
                        (long) i * 1000L,
                        "dbl_v",
                        i * 1.5,
                        "bool_v",
                        i % 2 == 0,
                        "date_v",
                        "2024-01-" + String.format(java.util.Locale.ROOT, "%02d", (i % 28) + 1)
                    )
                    .get()
                    .status()
            );
        }
    }

    /**
     * Indexes docs with some null values for nullable testing.
     */
    public static void indexNullableDocs(Client client, String index, int count, double nullRatio) {
        Random rng = new Random(42);
        for (int i = 0; i < count; i++) {
            Object kwVal = rng.nextDouble() < nullRatio ? null : "kw_" + i;
            Object intVal = rng.nextDouble() < nullRatio ? null : i;
            Object dblVal = rng.nextDouble() < nullRatio ? null : i * 2.5;
            assertEquals(
                RestStatus.CREATED,
                client.prepareIndex()
                    .setIndex(index)
                    .setId(String.valueOf(i))
                    .setSource("kw", kwVal, "int_v", intVal, "dbl_v", dblVal)
                    .get()
                    .status()
            );
        }
    }

    /**
     * Indexes docs sorted by 'kw' field for sorting_columns testing.
     */
    public static void indexSortedDocs(Client client, String index, int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client.prepareIndex()
                    .setIndex(index)
                    .setId(String.valueOf(i))
                    .setSource("kw", String.format("sort_%05d", i), "value", i)
                    .get()
                    .status()
            );
        }
    }

    /**
     * Indexes a large batch (creates multiple segments naturally via bulk).
     */
    public static void indexLargeBatch(Client client, String index, int count, int batchSize) {
        int offset = 0;
        while (offset < count) {
            BulkRequestBuilder bulk = client.prepareBulk();
            int end = Math.min(offset + batchSize, count);
            for (int i = offset; i < end; i++) {
                bulk.add(client.prepareIndex().setIndex(index).setId(String.valueOf(i)).setSource("name", "doc_" + i, "value", i));
            }
            bulk.get();
            offset = end;
        }
    }

    /**
     * Indexes docs distributed across shards via routing.
     */
    public static void indexAcrossShards(Client client, String index, int count, int shardCount) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client.prepareIndex()
                    .setIndex(index)
                    .setId(String.valueOf(i))
                    .setRouting(String.valueOf(i % shardCount))
                    .setSource("name", "doc_" + i, "value", i)
                    .get()
                    .status()
            );
        }
    }

    // --- Standard mappings ---

    /** Basic mixed mapping: name keyword, value integer */
    public static String basicMixedMapping() {
        return "{ \"properties\": { \"name\": { \"type\": \"keyword\" }, \"value\": { \"type\": \"integer\" } } }";
    }

    /** All primitive types mapping */
    public static String multiTypeMapping() {
        return "{ \"properties\": {"
            + " \"kw\": { \"type\": \"keyword\" },"
            + " \"int_v\": { \"type\": \"integer\" },"
            + " \"long_v\": { \"type\": \"long\" },"
            + " \"dbl_v\": { \"type\": \"double\" },"
            + " \"bool_v\": { \"type\": \"boolean\" },"
            + " \"date_v\": { \"type\": \"date\" }"
            + " } }";
    }

    /** Nullable fields mapping */
    public static String nullableMapping() {
        return "{ \"properties\": {"
            + " \"kw\": { \"type\": \"keyword\" },"
            + " \"int_v\": { \"type\": \"integer\" },"
            + " \"dbl_v\": { \"type\": \"double\" }"
            + " } }";
    }
}
