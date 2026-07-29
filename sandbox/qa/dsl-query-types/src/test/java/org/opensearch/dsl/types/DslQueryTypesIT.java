/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-query-type DSL integration test. One {@code resources/datasets/<type>/} folder per query type
 * ({@code mapping.json} + {@code bulk.json} + {@code dsl/q{N}.json}); each is provisioned into a
 * parquet/composite index via {@link DatasetProvisioner} and queried over HTTP against the live
 * sandbox server. The test JVM loads no plugins — it only sends {@code _search} requests.
 *
 * <p>Every {@code (query-type, queryNumber)} pair from {@link DslQueryTypeCatalog} is an
 * <b>independent</b> parameterized test (one instance per pair via {@link ParametersFactory}). A
 * failure in one pair — a provisioning rejection, a non-200, or a golden mismatch — fails only that
 * one test; every other pair still runs. This is deliberate: the suite maps what the parquet engine
 * can and cannot serve, so unsupported shapes (geo/nested, multi-valued keyword arrays, custom
 * {@code _id}s) show up as isolated reds rather than aborting the sweep.
 *
 * <p>Each test provisions the dataset, runs its query, asserts HTTP 200, and validates the response
 * against the expected answer at {@code dsl/expected/q{N}.json} via {@link DslResponseValidator}.
 * Expected answers are generated out-of-band on a vanilla OpenSearch index and committed; this suite
 * only validates against them.
 *
 * <pre>
 *   ./gradlew :sandbox:qa:dsl-query-types:restTest \
 *     --tests "org.opensearch.dsl.types.DslQueryTypesIT" -PrestCluster=localhost:9200
 * </pre>
 */
public class DslQueryTypesIT extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(DslQueryTypesIT.class);

    private final DslQueryTypeCatalog.Entry entry;
    private final int queryNumber;

    public DslQueryTypesIT(@Name("param") Param param) {
        this.entry = param.entry;
        this.queryNumber = param.queryNumber;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /** One parameterized test = one (query-type, queryNumber) pair. */
    public static final class Param {
        final DslQueryTypeCatalog.Entry entry;
        final int queryNumber;

        Param(DslQueryTypeCatalog.Entry entry, int queryNumber) {
            this.entry = entry;
            this.queryNumber = queryNumber;
        }

        @Override
        public String toString() {
            // Drives the per-test display name, e.g. "term/q1".
            return entry.type + "/q" + queryNumber;
        }
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for (DslQueryTypeCatalog.Entry entry : DslQueryTypeCatalog.all()) {
            List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(entry.dataset, "dsl");
            if (queryNumbers.isEmpty()) {
                logger.warn("No dsl/q*.json queries discovered for dataset [{}] — skipping", entry.type);
                continue;
            }
            for (int queryNumber : queryNumbers) {
                params.add(new Object[] { new Param(entry, queryNumber) });
            }
        }
        return params;
    }

    /**
     * Provision this pair's dataset on the parquet backend, run its query, and validate the response
     * against the committed golden. Any failure — provisioning rejection, non-200, or golden mismatch —
     * fails just this test.
     *
     * <p>Goldens are the TRUE (vanilla Lucene) answer, generated out-of-band and committed; parquet is
     * validated against them, so its deviations — null text {@code _source}, multi-valued keyword /
     * custom-id / geo / nested rejections — surface as red. See {@link DslResponseValidator}.
     */
    public void testQueryType() throws Exception {
        // A provisioning rejection (geo/nested mapping, multi-valued keyword, custom _id) is a real,
        // expected finding for unsupported shapes on parquet: it fails this one test, not the sweep.
        DatasetProvisioner.provision(client(), entry.dataset);

        String queryBody = DatasetProvisioner.loadResource(entry.dataset.queryResourcePath("dsl", "json", queryNumber));
        Request request = new Request("POST", "/" + entry.dataset.indexName + "/_search");
        request.setJsonEntity(queryBody);
        Response response = client().performRequest(request);
        assertEquals(
            "DSL " + entry.type + " Q" + queryNumber + ": expected HTTP 200",
            200,
            response.getStatusLine().getStatusCode()
        );
        Map<String, Object> actual = entityAsMap(response);

        String failure = DslResponseValidator.validate(entry.dataset, queryNumber, actual);
        assertNull(failure, failure);
    }
}
