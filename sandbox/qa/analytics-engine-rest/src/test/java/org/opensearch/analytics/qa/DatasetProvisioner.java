/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Generic provisioner that creates an index from a {@link Dataset} descriptor.
 * <p>
 * Reads {@code mapping.json} and {@code bulk.json} from the dataset's resource
 * directory and ingests them into the cluster. Idempotent — deletes the index
 * first if it already exists.
 * <p>
 * Applies parquet data format settings so the dataset is queryable via the
 * DataFusion backend.
 */
public final class DatasetProvisioner {

    private static final Logger logger = LogManager.getLogger(DatasetProvisioner.class);

    /**
     * How the dataset's documents are laid out into parquet segments per shard — a controlled axis
     * for plan-shape tests, where the shard DataFusion physical plan can legitimately differ with
     * segment count (e.g. the scan's {@code input_partitions}). The {@code suffix} disambiguates the
     * per-layout index name.
     */
    public enum SegmentLayout {
        /** Exactly one segment per shard: single bulk + flush, then force-merge to one segment. */
        SINGLE_SEGMENT("1seg"),
        /**
         * Exactly {@link #MULTI_SEGMENT_COUNT} segments per shard: bulk in that many flushed parts.
         * Parquet flush→segment is 1:1, so N parts give exactly N segments — deterministically pinning
         * the scan's {@code input_partitions}. No force-merge: it caps "at most N" and could collapse
         * tiny segments to 1; the default TieredMergePolicy won't auto-merge so few either.
         */
        MULTI_SEGMENT("nseg");

        /** Short tag for the per-layout index name (e.g. {@code parquet_hits_2s_1seg}). */
        public final String suffix;

        SegmentLayout(String suffix) {
            this.suffix = suffix;
        }
    }

    /** The per-shard segment count produced by {@link SegmentLayout#MULTI_SEGMENT} (one flush each). */
    public static final int MULTI_SEGMENT_COUNT = 2;

    private DatasetProvisioner() {
        // utility class
    }

    /**
     * Provision the dataset into the cluster with parquet as the primary data format. Segment layout
     * is left to the engine (single bulk + flush); pass a {@link SegmentLayout} to control it.
     */
    public static void provision(RestClient client, Dataset dataset, int numberOfShards) throws IOException {
        provision(client, dataset, numberOfShards, null);
    }

    /**
     * Provision the dataset, optionally pinning the per-shard segment layout via {@code layout}
     * ({@code null} = single bulk + flush, engine-decided segment count).
     */
    public static void provision(RestClient client, Dataset dataset, int numberOfShards, SegmentLayout layout) throws IOException {
        for (String indexName : dataset.indexNames) {
            provisionIndex(client, dataset, indexName, numberOfShards, layout);
        }
    }

    public static void provision(RestClient client, Dataset dataset) throws IOException {
        provision(client, dataset, 0);
    }

    /**
     * Provision one index. {@code numberOfShards} overrides the mapping's value ({@code 0} keeps it).
     * {@code layout} pins the per-shard segment layout ({@code null} = single bulk + flush, engine-
     * decided). Used by tests needing multi-shard / multi-segment coverage of planner paths.
     */
    private static void provisionIndex(RestClient client, Dataset dataset, String indexName, int numberOfShards, SegmentLayout layout)
        throws IOException {
        // Delete if exists
        try {
            client.performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception e) {
            // index may not exist — ignore
        }

        // Load mapping, inject parquet settings, create index
        String mappingPath = dataset.indexNames.size() == 1
            ? dataset.mappingResourcePath()
            : "datasets/" + dataset.name + "/mapping_" + indexName + ".json";
        String mapping = loadResource(mappingPath);
        String indexBody = injectParquetSettings(mapping);
        if (numberOfShards > 0) {
            indexBody = overrideNumberOfShards(indexBody, numberOfShards);
        }
        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(indexBody);
        client.performRequest(createIndex);

        // Bulk ingest. The segment layout decides how the rows are committed into parquet segments.
        String bulkPath = dataset.indexNames.size() == 1
            ? dataset.bulkResourcePath()
            : "datasets/" + dataset.name + "/bulk_" + indexName + ".json";
        String bulkBody = loadResource(bulkPath);

        if (layout == SegmentLayout.MULTI_SEGMENT) {
            // Split the ndjson into MULTI_SEGMENT_COUNT parts at action/source boundaries; flush
            // after each. Each flush is one parquet segment (1:1), so every shard ends up with
            // exactly that many segments. No force-merge — it would only risk collapsing them
            // (see SegmentLayout.MULTI_SEGMENT). Background merge leaves so few segments alone.
            for (String part : splitNdjson(bulkBody, MULTI_SEGMENT_COUNT)) {
                bulkAndFlush(client, indexName, part);
            }
        } else {
            bulkAndFlush(client, indexName, bulkBody);
            if (layout == SegmentLayout.SINGLE_SEGMENT) {
                // Collapse every shard to exactly one parquet segment so the shard physical plan is
                // deterministic (no per-shard divergence from differing segment counts).
                forceMergeAndFlush(client, indexName, 1);
            }
            // layout == null: leave segment count to the engine (legacy non-plan-shape callers).
        }

        // Wait for index health. wait_for_status=yellow only guarantees primaries are assigned, not
        // that every shard copy is active and done initializing — on a multi-node cluster a search
        // can then race ahead of the shard becoming searchable and fail with "no such shard". Wait
        // for green + all shards active + none initializing so the first query always finds them.
        Request healthRequest = new Request("GET", "/_cluster/health/" + indexName);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("wait_for_active_shards", "all");
        healthRequest.addParameter("wait_for_no_initializing_shards", "true");
        healthRequest.addParameter("wait_for_no_relocating_shards", "true");
        healthRequest.addParameter("timeout", "60s");
        client.performRequest(healthRequest);

        logger.info("Dataset [{}] provisioned into index [{}]", dataset.name, indexName);
    }

    /** Bulk-ingest one ndjson body (refresh=true) and force a flush so its segment is committed. */
    private static void bulkAndFlush(RestClient client, String indexName, String ndjson) throws IOException {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(ndjson);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Response bulkResponse = client.performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());
        String responseBody = new String(bulkResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        logger.info("Bulk response for index [{}]: {}", indexName, responseBody);

        Request flushRequest = new Request("POST", "/" + indexName + "/_flush");
        flushRequest.addParameter("force", "true");
        client.performRequest(flushRequest);
    }

    /** Force-merge every shard to exactly {@code maxSegments} parquet segments, then flush. */
    private static void forceMergeAndFlush(RestClient client, String indexName, int maxSegments) throws IOException {
        Request merge = new Request("POST", "/" + indexName + "/_forcemerge");
        merge.addParameter("max_num_segments", Integer.toString(maxSegments));
        client.performRequest(merge);
        Request flush = new Request("POST", "/" + indexName + "/_flush");
        flush.addParameter("force", "true");
        client.performRequest(flush);
    }

    /**
     * Split an ndjson bulk body into {@code parts} non-empty chunks at action/source line
     * boundaries. The bulk format alternates an action line ({@code {"index":{}}}) and a source
     * line, so every cut must land on an even document boundary to keep each chunk self-contained.
     * Each chunk, flushed on its own, becomes one parquet segment.
     */
    private static List<String> splitNdjson(String ndjson, int parts) {
        List<String> docLines = new ArrayList<>();
        for (String line : ndjson.split("\n")) {
            if (!line.isEmpty()) {
                docLines.add(line);
            }
        }
        int pairCount = docLines.size() / 2; // (action, source) pairs
        // Each part is flushed into its own segment, so we need at least one doc per part — otherwise
        // we'd silently produce fewer segments than requested and the shard plan's input_partitions
        // wouldn't match the golden. Fail loudly instead.
        if (pairCount < parts) {
            throw new IllegalArgumentException(
                "dataset has " + pairCount + " doc(s), too few for a " + parts + "-segment layout (need >= " + parts + ")"
            );
        }
        int pairsPerPart = Math.max(1, (int) Math.ceil((double) pairCount / parts));
        List<String> chunks = new ArrayList<>();
        StringBuilder chunk = new StringBuilder();
        int pairsInChunk = 0;
        for (int i = 0; i < docLines.size(); i += 2) {
            chunk.append(docLines.get(i)).append('\n');
            if (i + 1 < docLines.size()) {
                chunk.append(docLines.get(i + 1)).append('\n');
            }
            if (++pairsInChunk == pairsPerPart && chunks.size() < parts - 1) {
                chunks.add(chunk.toString());
                chunk = new StringBuilder();
                pairsInChunk = 0;
            }
        }
        if (chunk.length() > 0) {
            chunks.add(chunk.toString());
        }
        return chunks;
    }

    // TODO(plan-shape): both this and injectParquetSettings mutate the index settings by string/regex
    // rewriting the raw mapping JSON — brittle (depends on the literal "number_of_shards" token) and
    // it means a combo's SettingsCombo.indexSettings map can't drive arbitrary index knobs. Replace
    // with: parse mapping JSON -> merge a settings map (mapping defaults + parquet + combo.indexSettings)
    // -> re-serialize. Shared by ~15 ITs, so do it as its own change and re-verify them.
    /**
     * Replace the {@code number_of_shards} value in the mapping body. Matches the form
     * {@code "number_of_shards": <int>} produced by the canonical dataset mappings.
     */
    private static String overrideNumberOfShards(String mappingBody, int numberOfShards) {
        return mappingBody.replaceAll("\"number_of_shards\"\\s*:\\s*\\d+", "\"number_of_shards\": " + numberOfShards);
    }

    /**
     * Inject parquet data format settings into the existing settings block.
     *
     * <p>Lucene is set as the secondary format so the Lucene analytics backend is available
     * for text-search functions (match, match_phrase, query_string, ...). Without it those
     * functions fail at planning time with
     * {@code "No backend can evaluate filter predicate [OTHER_FUNCTION] on fields [...:text]"}
     * because the Lucene backend never gets enrolled as a candidate.
     */
    private static String injectParquetSettings(String mappingBody) {
        return mappingBody.replace(
            "\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
                + "\"index.pluggable.dataformat\": \"composite\", "
                + "\"index.composite.primary_data_format\": \"parquet\", "
                + "\"index.composite.secondary_data_formats\": [\"lucene\"], "
                + "\"number_of_shards\""
        );
    }

    /**
     * Load a classpath resource as a UTF-8 string.
     */
    public static String loadResource(String path) throws IOException {
        try (InputStream is = DatasetProvisioner.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String content = reader.lines().collect(Collectors.joining("\n"));
                return content.isEmpty() ? content : content + "\n";
            }
        }
    }
}
