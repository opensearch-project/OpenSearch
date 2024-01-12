/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.client.Client;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpInfo;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.OpenSearchIntegTestCase.SuiteScopeTestCase;
import org.opensearch.test.client.RandomizingClient;
import org.opensearch.test.telemetry.tracing.StrictCheckSpanProcessor;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class OpenSearchTestClusterRule implements MethodRule {
    private final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the suite cluster.
     */
    private TestCluster currentCluster;
    private RestClient restClient = null;

    private OpenSearchIntegTestCase INSTANCE = null; // see @SuiteScope
    private Long SUITE_SEED = null;

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return statement(base, method, target);
    }

    private Statement statement(final Statement base, FrameworkMethod method, Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before(target);

                List<Throwable> errors = new ArrayList<Throwable>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        after(target);
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    private void initializeSuiteScope(OpenSearchIntegTestCase target) throws Exception {
        Class<?> targetClass = OpenSearchTestCase.getTestClass();
        /*
          Note we create these test class instance via reflection
          since JUnit creates a new instance per test and that is also
          the reason why INSTANCE is static since this entire method
          must be executed in a static context.
         */
        if (INSTANCE != null) {
            return;
        }

        if (isSuiteScopedTest(targetClass)) {
            INSTANCE = target;

            boolean success = false;
            try {
                printTestMessage("setup");
                beforeInternal(target);
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    protected final void beforeInternal(OpenSearchIntegTestCase target) throws Exception {
        final Scope currentClusterScope = getClusterScope(target.getClass());
        Callable<Void> setup = () -> {
            currentCluster.beforeTest(OpenSearchTestCase.random());
            currentCluster.wipe(target.excludeTemplates());
            target.randomIndexTemplate();
            return null;
        };
        switch (currentClusterScope) {
            case SUITE:
                assert SUITE_SEED != null : "Suite seed was not initialized";
                currentCluster = buildAndPutCluster(currentClusterScope, SUITE_SEED, target);
                RandomizedContext.current().runWithPrivateRandomness(SUITE_SEED, setup);
                break;
            case TEST:
                currentCluster = buildAndPutCluster(currentClusterScope, OpenSearchTestCase.randomLong(), target);
                setup.call();
                break;
        }

    }

    protected void before(Object target) throws Throwable {
        final OpenSearchIntegTestCase instance = (OpenSearchIntegTestCase) target;
        initializeSuiteScope(instance);

        if (runTestScopeLifecycle()) {
            printTestMessage("setting up");
            beforeInternal(instance);
            printTestMessage("all set up");
        }
    }

    protected void after(Object target) throws Exception {
        final OpenSearchIntegTestCase instance = (OpenSearchIntegTestCase) target;

        // Deleting indices is going to clear search contexts implicitly so we
        // need to check that there are no more in-flight search contexts before
        // we remove indices
        if (isInternalCluster()) {
            internalCluster().setBootstrapClusterManagerNodeIndex(-1);
        }
        instance.ensureAllSearchContextsReleased();
        if (runTestScopeLifecycle()) {
            printTestMessage("cleaning up after");
            afterInternal(false, instance);
            printTestMessage("cleaned up after");
        }
    }

    public void beforeClass() throws Exception {
        SUITE_SEED = OpenSearchTestCase.randomLong();
    }

    public void afterClass() throws Exception {
        try {
            if (runTestScopeLifecycle()) {
                clearClusters();
            } else {
                printTestMessage("cleaning up after");
                afterInternal(true, null);
                OpenSearchTestCase.checkStaticState(true);
            }
            StrictCheckSpanProcessor.validateTracingStateOnShutdown();
        } finally {
            SUITE_SEED = null;
            currentCluster = null;
            INSTANCE = null;
        }
    }

    private boolean runTestScopeLifecycle() {
        return INSTANCE == null;
    }

    private TestCluster buildAndPutCluster(Scope currentClusterScope, long seed, OpenSearchIntegTestCase target) throws Exception {
        final Class<?> clazz = target.getClass();

        synchronized (clusters) {
            TestCluster testCluster = clusters.remove(clazz); // remove this cluster first
            clearClusters(); // all leftovers are gone by now... this is really just a double safety if we miss something somewhere
            switch (currentClusterScope) {
                case SUITE:
                    if (testCluster == null) { // only build if it's not there yet
                        testCluster = buildWithPrivateContext(currentClusterScope, seed, target);
                    }
                    break;
                case TEST:
                    // close the previous one and create a new one
                    IOUtils.closeWhileHandlingException(testCluster);
                    testCluster = target.buildTestCluster(currentClusterScope, seed);
                    break;
            }
            clusters.put(clazz, testCluster);
            return testCluster;
        }
    }

    private void printTestMessage(String message) {
        logger.info("[{}]: {} suite", OpenSearchTestCase.getTestClass().getSimpleName(), message);
    }

    private void afterInternal(boolean afterClass, OpenSearchIntegTestCase target) throws Exception {
        final Scope currentClusterScope = getClusterScope(OpenSearchTestCase.getTestClass());
        if (isInternalCluster()) {
            internalCluster().clearDisruptionScheme();
        }

        OpenSearchIntegTestCase instance = INSTANCE;
        if (instance == null) {
            instance = target;
        }

        try {
            if (cluster() != null) {
                if (currentClusterScope != Scope.TEST) {
                    Metadata metadata = client().admin().cluster().prepareState().execute().actionGet().getState().getMetadata();

                    final Set<String> persistentKeys = new HashSet<>(metadata.persistentSettings().keySet());
                    assertThat("test leaves persistent cluster metadata behind", persistentKeys, empty());

                    final Set<String> transientKeys = new HashSet<>(metadata.transientSettings().keySet());
                    assertThat("test leaves transient cluster metadata behind", transientKeys, empty());
                }
                instance.ensureClusterSizeConsistency();
                instance.ensureClusterStateConsistency();
                instance.ensureClusterStateCanBeReadByNodeTool();
                instance.beforeIndexDeletion();
                cluster().wipe(instance.excludeTemplates()); // wipe after to make sure we fail in the test that didn't ack the delete
                if (afterClass || currentClusterScope == Scope.TEST) {
                    cluster().close();
                }
                cluster().assertAfterTest();
            }
        } finally {
            if (currentClusterScope == Scope.TEST) {
                clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
            }
        }
    }

    private void clearClusters() throws Exception {
        synchronized (clusters) {
            if (!clusters.isEmpty()) {
                IOUtils.close(clusters.values());
                clusters.clear();
            }
        }
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
        OpenSearchTestCase.assertBusy(() -> {
            int numChannels = RestCancellableNodeClient.getNumChannels();
            OpenSearchTestCase.assertEquals(
                numChannels
                    + " channels still being tracked in "
                    + RestCancellableNodeClient.class.getSimpleName()
                    + " while there should be none",
                0,
                numChannels
            );
        });
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed, OpenSearchIntegTestCase target) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, () -> target.buildTestCluster(scope, seed));
    }

    private static boolean isSuiteScopedTest(Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTestCase.class) != null;
    }

    public TestCluster cluster() {
        return currentCluster;
    }

    public boolean isInternalCluster() {
        return (cluster() instanceof InternalTestCluster);
    }

    private Scope getClusterScope(Class<?> clazz) {
        ClusterScope annotation = OpenSearchIntegTestCase.getAnnotation(clazz, ClusterScope.class);
        // if we are not annotated assume suite!
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    public InternalTestCluster internalCluster() {
        if (!isInternalCluster()) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (InternalTestCluster) cluster();
    }

    public Client client() {
        return client(null);
    }

    public Client client(@Nullable String node) {
        if (node != null) {
            return internalCluster().client(node);
        }
        Client client = cluster().client();
        if (OpenSearchTestCase.frequently()) {
            client = new RandomizingClient(client, OpenSearchTestCase.random());
        }
        return client;
    }

    public synchronized RestClient getRestClient() {
        if (restClient == null) {
            restClient = createRestClient();
        }
        return restClient;
    }

    protected RestClient createRestClient() {
        return createRestClient(null, "http");
    }

    protected RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        return createRestClient(nodesInfoResponse.getNodes(), httpClientConfigCallback, protocol);
    }

    protected RestClient createRestClient(
        final List<NodeInfo> nodes,
        RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback,
        String protocol
    ) {
        List<HttpHost> hosts = new ArrayList<>();
        for (NodeInfo node : nodes) {
            if (node.getInfo(HttpInfo.class) != null) {
                TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                hosts.add(new HttpHost(protocol, NetworkAddress.format(address.getAddress()), address.getPort()));
            }
        }
        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
        if (httpClientConfigCallback != null) {
            builder.setHttpClientConfigCallback(httpClientConfigCallback);
        }
        return builder.build();
    }

}
