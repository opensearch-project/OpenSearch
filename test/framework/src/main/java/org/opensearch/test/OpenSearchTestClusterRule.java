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
import org.opensearch.transport.client.Client;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * The JUnit {@link MethodRule} that handles test method scoped and test suite scoped clusters for integration (internal cluster) tests. There rule is
 * injected into {@link OpenSearchIntegTestCase} that every integration test suite should be subclassing. In case of the parameterized test suites,
 * please subclass {@link ParameterizedStaticSettingsOpenSearchIntegTestCase} or {@link ParameterizedDynamicSettingsOpenSearchIntegTestCase}, depending
 * on the way cluster settings are being managed.
 */
class OpenSearchTestClusterRule implements MethodRule {
    // Maps each TestCluster instance to the exact test suite instance that triggered its creation
    private final Map<TestCluster, OpenSearchIntegTestCase> suites = new IdentityHashMap<>();
    private final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the suite cluster.
     */
    private TestCluster currentCluster = null;
    private RestClient restClient = null;

    private OpenSearchIntegTestCase suiteInstance = null; // see @SuiteScope
    private Long suiteSeed = null;

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return statement(base, method, target);
    }

    void beforeClass() throws Exception {
        suiteSeed = OpenSearchTestCase.randomLong();
    }

    void afterClass() throws Exception {
        try {
            if (runTestScopeLifecycle()) {
                clearClusters();
            } else {
                printTestMessage("cleaning up after");
                afterInternal(true, null);
                OpenSearchTestCase.checkStaticState(true);
                synchronized (clusters) {
                    final TestCluster cluster = clusters.remove(getTestClass());
                    IOUtils.closeWhileHandlingException(cluster);
                    if (cluster != null) {
                        suites.remove(cluster);
                    }
                }
            }
            StrictCheckSpanProcessor.validateTracingStateOnShutdown();
        } finally {
            suiteSeed = null;
            currentCluster = null;
            suiteInstance = null;
        }
    }

    TestCluster cluster() {
        return currentCluster;
    }

    boolean isInternalCluster() {
        return (cluster() instanceof InternalTestCluster);
    }

    Optional<InternalTestCluster> internalCluster() {
        if (!isInternalCluster()) {
            return Optional.empty();
        } else {
            return Optional.of((InternalTestCluster) cluster());
        }
    }

    Client clientForAnyNode() {
        return clientForNode(null);
    }

    Client clientForNode(@Nullable String node) {
        if (node != null) {
            return internalCluster().orElseThrow(() -> new UnsupportedOperationException("current test cluster is immutable")).client(node);
        }
        Client client = cluster().client();
        if (OpenSearchTestCase.frequently()) {
            client = new RandomizingClient(client, OpenSearchTestCase.random());
        }
        return client;
    }

    synchronized RestClient getRestClient() {
        if (restClient == null) {
            restClient = createRestClient();
        }
        return restClient;
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
                assert suiteSeed != null : "Suite seed was not initialized";
                currentCluster = buildAndPutCluster(currentClusterScope, suiteSeed, target);
                RandomizedContext.current().runWithPrivateRandomness(suiteSeed, setup);
                break;
            case TEST:
                currentCluster = buildAndPutCluster(currentClusterScope, OpenSearchTestCase.randomLong(), target);
                setup.call();
                break;
        }
    }

    protected void before(Object target, FrameworkMethod method) throws Throwable {
        final OpenSearchIntegTestCase instance = (OpenSearchIntegTestCase) target;
        initializeSuiteScope(instance, method);

        if (runTestScopeLifecycle()) {
            printTestMessage("setting up", method);
            beforeInternal(instance);
            printTestMessage("all set up", method);
        }
    }

    protected void after(Object target, FrameworkMethod method) throws Exception {
        final OpenSearchIntegTestCase instance = (OpenSearchIntegTestCase) target;

        // Deleting indices is going to clear search contexts implicitly so we
        // need to check that there are no more in-flight search contexts before
        // we remove indices
        internalCluster().ifPresent(c -> c.setBootstrapClusterManagerNodeIndex(-1));

        instance.ensureAllSearchContextsReleased();
        if (runTestScopeLifecycle()) {
            printTestMessage("cleaning up after", method);
            afterInternal(false, instance);
            printTestMessage("cleaned up after", method);
        }
    }

    protected RestClient createRestClient() {
        return createRestClient(null, "http");
    }

    protected RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        NodesInfoResponse nodesInfoResponse = clientForAnyNode().admin().cluster().prepareNodesInfo().get();
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

    private Scope getClusterScope(Class<?> clazz) {
        ClusterScope annotation = OpenSearchIntegTestCase.getAnnotation(clazz, ClusterScope.class);
        // if we are not annotated assume suite!
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed, OpenSearchIntegTestCase target) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, () -> target.buildTestCluster(scope, seed));
    }

    private static boolean isSuiteScopedTest(Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTestCase.class) != null;
    }

    private static boolean hasParametersChanged(
        final ParameterizedOpenSearchIntegTestCase instance,
        final ParameterizedOpenSearchIntegTestCase target
    ) {
        return !instance.hasSameParametersAs(target);
    }

    private boolean runTestScopeLifecycle() {
        return suiteInstance == null;
    }

    private TestCluster buildAndPutCluster(Scope currentClusterScope, long seed, OpenSearchIntegTestCase target) throws Exception {
        final Class<?> clazz = target.getClass();

        synchronized (clusters) {
            TestCluster testCluster = clusters.remove(clazz); // remove this cluster first
            clearClusters(); // all leftovers are gone by now... this is really just a double safety if we miss something somewhere
            switch (currentClusterScope) {
                case SUITE:
                    if (testCluster != null && target instanceof ParameterizedOpenSearchIntegTestCase) {
                        final OpenSearchIntegTestCase instance = suites.get(testCluster);
                        if (instance != null) {
                            assert instance instanceof ParameterizedOpenSearchIntegTestCase;
                            if (hasParametersChanged(
                                (ParameterizedOpenSearchIntegTestCase) instance,
                                (ParameterizedOpenSearchIntegTestCase) target
                            )) {
                                IOUtils.closeWhileHandlingException(testCluster);
                                printTestMessage("new instance of parameterized test class, recreating test cluster for suite");
                                testCluster = null;
                            }
                        }
                    }

                    if (testCluster == null) { // only build if it's not there yet
                        testCluster = buildWithPrivateContext(currentClusterScope, seed, target);
                        suites.put(testCluster, target);
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
        logger.info("[{}]: {} suite", getTestClass().getSimpleName(), message);
    }

    private static Class<?> getTestClass() {
        return OpenSearchTestCase.getTestClass();
    }

    private void printTestMessage(String message, FrameworkMethod method) {
        logger.info("[{}#{}]: {} test", getTestClass().getSimpleName(), method.getName(), message);
    }

    private void afterInternal(boolean afterClass, OpenSearchIntegTestCase target) throws Exception {
        final Scope currentClusterScope = getClusterScope(getTestClass());
        internalCluster().ifPresent(InternalTestCluster::clearDisruptionScheme);

        OpenSearchIntegTestCase instance = suiteInstance;
        if (instance == null) {
            instance = target;
        }

        try {
            if (cluster() != null) {
                if (currentClusterScope != Scope.TEST) {
                    Metadata metadata = clientForAnyNode().admin().cluster().prepareState().execute().actionGet().getState().getMetadata();

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
                suites.clear();
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

    private Statement statement(final Statement base, FrameworkMethod method, Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before(target, method);

                List<Throwable> errors = new ArrayList<Throwable>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        after(target, method);
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    private void initializeSuiteScope(OpenSearchIntegTestCase target, FrameworkMethod method) throws Exception {
        final Class<?> targetClass = getTestClass();
        /*
          Note we create these test class instance via reflection
          since JUnit creates a new instance per test.
         */
        if (suiteInstance != null) {
            // Catching the case when parameterized test cases are run: the test class stays the same but the test instances changes.
            if (target instanceof ParameterizedOpenSearchIntegTestCase) {
                assert suiteInstance instanceof ParameterizedOpenSearchIntegTestCase;
                if (hasParametersChanged(
                    (ParameterizedOpenSearchIntegTestCase) suiteInstance,
                    (ParameterizedOpenSearchIntegTestCase) target
                )) {
                    printTestMessage("new instance of parameterized test class, recreating cluster scope", method);
                    afterClass();
                    beforeClass();
                } else {
                    return; /* same test class instance */
                }
            } else {
                return; /* not a parameterized test */
            }
        }

        assert suiteInstance == null;
        if (isSuiteScopedTest(targetClass)) {
            suiteInstance = target;

            boolean success = false;
            try {
                printTestMessage("setup", method);
                beforeInternal(target);
                suiteInstance.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            suiteInstance = null;
        }
    }
}
