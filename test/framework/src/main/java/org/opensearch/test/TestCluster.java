/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.IndexTemplateMissingException;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Base test cluster that exposes the basis to run tests against any opensearch cluster, whose layout
 * (e.g. number of nodes) is predefined and cannot be changed during the tests execution
 */
public abstract class TestCluster implements Closeable {

    protected final Logger logger = LogManager.getLogger(getClass());
    private final long seed;

    protected Random random;

    public TestCluster(long seed) {
        this.seed = seed;
    }

    public long seed() {
        return seed;
    }

    /**
     * This method should be executed before each test to reset the cluster to its initial state.
     */
    public void beforeTest(Random random) throws IOException, InterruptedException {
        this.random = new Random(random.nextLong());
    }

    /**
     * Wipes any data that a test can leave behind: indices, templates (except exclude templates) and repositories
     */
    public void wipe(Set<String> excludeTemplates) {
        wipeAllDataStreams();
        wipeIndices("_all");
        wipeAllTemplates(excludeTemplates);
        wipeRepositories();
    }

    /**
     * Assertions that should run before the cluster is wiped should be called in this method
     */
    public void beforeIndexDeletion() throws Exception {}

    /**
     * This method checks all the things that need to be checked after each test
     */
    public void assertAfterTest() throws Exception {
        ensureEstimatedStats();
    }

    /**
     * This method should be executed during tear down, after each test (but after assertAfterTest)
     */
    public abstract void afterTest() throws IOException;

    /**
     * Returns a client connected to any node in the cluster
     */
    public abstract Client client();

    /**
     * Returns the number of nodes in the cluster.
     */
    public abstract int size();

    /**
     * Returns the number of data nodes in the cluster.
     */
    public abstract int numDataNodes();

    /**
     * Returns the number of data and cluster-manager eligible nodes in the cluster.
     */
    // TODO: Add abstract keyword after removing the deprecated numDataAndMasterNodes()
    public int numDataAndClusterManagerNodes() {
        return numDataAndMasterNodes();
    }

    /**
     * Returns the number of data and cluster-manager eligible nodes in the cluster.
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #numDataAndClusterManagerNodes()}
     */
    @Deprecated
    public int numDataAndMasterNodes() {
        throw new UnsupportedOperationException("Must be overridden");
    }

    /**
     * Returns the http addresses of the nodes within the cluster.
     * Can be used to run REST tests against the test cluster.
     */
    public abstract InetSocketAddress[] httpAddresses();

    /**
     * Closes the current cluster
     */
    @Override
    public abstract void close() throws IOException;

    /**
     * Deletes all data streams from the test cluster.
     */
    public void wipeAllDataStreams() {
        // Feature flag may not be enabled in all gradle modules that use OpenSearchIntegTestCase
        if (size() > 0) {
            AcknowledgedResponse response = client().admin()
                .indices()
                .deleteDataStream(new DeleteDataStreamAction.Request(new String[] { "*" }))
                .actionGet();
            OpenSearchAssertions.assertAcked(response);
        }
    }

    /**
     * Deletes the given indices from the tests cluster. If no index name is passed to this method
     * all indices are removed.
     */
    public void wipeIndices(String... indices) {
        assert indices != null && indices.length > 0;
        if (size() > 0) {
            try {
                // include wiping hidden indices!
                OpenSearchAssertions.assertAcked(
                    client().admin()
                        .indices()
                        .prepareDelete(indices)
                        .setIndicesOptions(IndicesOptions.fromOptions(false, true, true, true, true, false, false, true, false))
                );
            } catch (IndexNotFoundException e) {
                // ignore
            } catch (IllegalArgumentException e) {
                // Happens if `action.destructive_requires_name` is set to true
                // which is the case in the CloseIndexDisableCloseAllTests
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    List<String> concreteIndices = new ArrayList<>();
                    for (IndexMetadata indexMetadata : clusterStateResponse.getState().metadata()) {
                        concreteIndices.add(indexMetadata.getIndex().getName());
                    }
                    if (!concreteIndices.isEmpty()) {
                        OpenSearchAssertions.assertAcked(
                            client().admin().indices().prepareDelete(concreteIndices.toArray(new String[concreteIndices.size()]))
                        );
                    }
                }
            }
        }
    }

    /**
     * Removes all templates, except the templates defined in the exclude
     */
    public void wipeAllTemplates(Set<String> exclude) {
        if (size() > 0) {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
            for (IndexTemplateMetadata indexTemplate : response.getIndexTemplates()) {
                if (exclude.contains(indexTemplate.getName())) {
                    continue;
                }
                try {
                    client().admin().indices().prepareDeleteTemplate(indexTemplate.getName()).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Deletes index templates, support wildcard notation.
     * If no template name is passed to this method all templates are removed.
     */
    public void wipeTemplates(String... templates) {
        if (size() > 0) {
            // if nothing is provided, delete all
            if (templates.length == 0) {
                templates = new String[] { "*" };
            }
            for (String template : templates) {
                try {
                    client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public void wipeRepositories(String... repositories) {
        if (size() > 0) {
            // if nothing is provided, delete all
            if (repositories.length == 0) {
                repositories = new String[] { "*" };
            }
            for (String repository : repositories) {
                try {
                    client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
                } catch (RepositoryMissingException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Ensures that any breaker statistics are reset to 0.
     *
     * The implementation is specific to the test cluster, because the act of
     * checking some breaker stats can increase them.
     */
    public abstract void ensureEstimatedStats();

    /**
     * Returns the cluster name
     */
    public abstract String getClusterName();

    /**
     * Returns an {@link Iterable} over all clients in this test cluster
     */
    public abstract Iterable<Client> getClients();

    /**
     * Returns this clusters {@link NamedWriteableRegistry} this is needed to
     * deserialize binary content from this cluster that might include custom named writeables
     */
    public abstract NamedWriteableRegistry getNamedWriteableRegistry();
}
