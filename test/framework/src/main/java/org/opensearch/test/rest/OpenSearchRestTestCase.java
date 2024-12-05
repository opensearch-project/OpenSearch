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

package org.opensearch.test.rest;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RequestOptions.Builder;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;

/**
 * Superclass for tests that interact with an external test cluster using OpenSearch's {@link RestClient}.
 */
public abstract class OpenSearchRestTestCase extends OpenSearchTestCase {
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";
    public static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";
    public static final String CLIENT_PATH_PREFIX = "client.path.prefix";

    // This set will contain the warnings already asserted since we are eliminating logging duplicate warnings.
    // This ensures that no matter in what order the tests run, the warning is asserted once.
    private static Set<String> assertedWarnings = ConcurrentHashMap.newKeySet();

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            return parser.map();
        }
    }

    /**
     * Convert the entity from a {@link Response} into a list of maps.
     */
    public static List<Object> entityAsList(Response response) throws IOException {
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            return parser.list();
        }
    }

    private static List<HttpHost> clusterHosts;
    /**
     * A client for the running OpenSearch cluster
     */
    private static RestClient client;
    /**
     * A client for the running OpenSearch cluster configured to take test administrative actions like remove all indexes after the test
     * completes
     */
    private static RestClient adminClient;
    private static TreeSet<Version> nodeVersions;

    @Before
    public void initClient() throws IOException {
        if (client == null) {
            assert adminClient == null;
            assert clusterHosts == null;
            assert nodeVersions == null;
            String cluster = getTestRestCluster();
            String[] stringUrls = cluster.split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
                hosts.add(buildHttpHost(host, port));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST clients against {}", clusterHosts);
            client = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
            adminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[0]));

            nodeVersions = new TreeSet<>();
            Map<?, ?> response = entityAsMap(adminClient.performRequest(new Request("GET", "_nodes/plugins")));
            Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
            for (Map.Entry<?, ?> node : nodes.entrySet()) {
                Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
                nodeVersions.add(Version.fromString(nodeInfo.get("version").toString()));
            }
        }
        assert client != null;
        assert adminClient != null;
        assert clusterHosts != null;
        assert nodeVersions != null;
    }

    protected String getTestRestCluster() {
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException(
                "Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests"
            );
        }
        return cluster;
    }

    /**
     * Helper class to check warnings in REST responses with sensitivity to versions
     * used in the target cluster.
     */
    public static class VersionSensitiveWarningsHandler implements WarningsHandler {
        Set<String> requiredSameVersionClusterWarnings = new HashSet<>();
        Set<String> allowedWarnings = new HashSet<>();
        final Set<Version> testNodeVersions;

        public VersionSensitiveWarningsHandler(Set<Version> nodeVersions) {
            this.testNodeVersions = nodeVersions;
        }

        /**
         * Adds to the set of warnings that are all required in responses if the cluster
         * is formed from nodes all running the exact same version as the client.
         * @param requiredWarnings a set of required warnings
         */
        public void current(String... requiredWarnings) {
            requiredSameVersionClusterWarnings.addAll(Arrays.asList(requiredWarnings));
        }

        /**
         * Adds to the set of warnings that are permissible (but not required) when running
         * in mixed-version clusters or those that differ in version from the test client.
         * @param allowedWarnings optional warnings that will be ignored if received
         */
        public void compatible(String... allowedWarnings) {
            this.allowedWarnings.addAll(Arrays.asList(allowedWarnings));
        }

        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            if (warnings.isEmpty()) {
                return false;
            }
            if (isExclusivelyTargetingCurrentVersionCluster()) {
                // absolute equality required in expected and actual.
                Set<String> actual = new HashSet<>(warnings);
                return false == requiredSameVersionClusterWarnings.equals(actual);
            } else {
                // Some known warnings can safely be ignored
                for (String actualWarning : warnings) {
                    if (false == allowedWarnings.contains(actualWarning)
                        && false == requiredSameVersionClusterWarnings.contains(actualWarning)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private boolean isExclusivelyTargetingCurrentVersionCluster() {
            assertFalse("Node versions running in the cluster are missing", testNodeVersions.isEmpty());
            return testNodeVersions.size() == 1 && testNodeVersions.iterator().next().equals(Version.CURRENT);
        }

    }

    public static RequestOptions expectVersionSpecificWarnings(Consumer<VersionSensitiveWarningsHandler> expectationsSetter) {
        Builder builder = RequestOptions.DEFAULT.toBuilder();
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(nodeVersions);
        expectationsSetter.accept(warningsHandler);
        builder.setWarningsHandler(warningsHandler);
        return builder.build();
    }

    /**
     * Creates request options designed to be used when making a call that can return warnings, for example a
     * deprecated request. The options will ensure that the given warnings are returned if all nodes are on
     * {@link Version#CURRENT} and will allow (but not require) the warnings if any node is running an older version.
     *
     * @param warnings The expected warnings.
     */
    public static RequestOptions expectWarnings(String... warnings) {
        return expectVersionSpecificWarnings(consumer -> consumer.current(warnings));
    }

    /**
     * Filters out already asserted warnings and calls expectWarnings method.
     * @param deprecationWarning expected warning
     */
    public static RequestOptions expectWarningsOnce(String deprecationWarning) {
        if (assertedWarnings.contains(deprecationWarning)) {
            return RequestOptions.DEFAULT;
        }
        assertedWarnings.add(deprecationWarning);
        return expectWarnings(deprecationWarning);
    }

    /**
     * Creates RequestOptions designed to ignore [types removal] warnings but nothing else
     * @deprecated this method is only required while we deprecate types and can be removed in 8.0
     */
    @Deprecated
    public static RequestOptions allowTypesRemovalWarnings() {
        Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(new WarningsHandler() {
            @Override
            public boolean warningsShouldFailRequest(List<String> warnings) {
                for (String warning : warnings) {
                    if (warning.startsWith("[types removal]") == false) {
                        // Something other than a types removal message - return true
                        return true;
                    }
                }
                return false;
            }
        });
        return builder.build();
    }

    /**
     * Construct an HttpHost from the given host and port
     */
    protected HttpHost buildHttpHost(String host, int port) {
        return new HttpHost(getProtocol(), host, port);
    }

    /**
     * Clean up after the test case.
     */
    @After
    public final void cleanUpCluster() throws Exception {
        if (preserveClusterUponCompletion() == false) {
            ensureNoInitializingShards();
            wipeCluster();
            waitForClusterStateUpdatesToFinish();
            logIfThereAreRunningTasks();
        }
    }

    @AfterClass
    public static void closeClients() throws IOException {
        try {
            IOUtils.close(client, adminClient);
        } finally {
            clusterHosts = null;
            client = null;
            adminClient = null;
            nodeVersions = null;
        }
    }

    /**
     * Get the client used for ordinary api calls while writing a test
     */
    protected static RestClient client() {
        return client;
    }

    /**
     * Get the client used for test administrative actions. Do not use this while writing a test. Only use it for cleaning up after tests.
     */
    protected static RestClient adminClient() {
        return adminClient;
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete.
     *
     * @param adminClient the admin client
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient adminClient) throws Exception {
        waitForPendingTasks(adminClient, taskName -> false);
    }

    /**
     * Wait for outstanding tasks to complete. The specified admin client is used to check the outstanding tasks and this is done using
     * {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} to give a chance to any outstanding tasks to complete. The specified
     * filter is used to filter out outstanding tasks that are expected to be there.
     *
     * @param adminClient the admin client
     * @param taskFilter  predicate used to filter tasks that are expected to be there
     * @throws Exception if an exception is thrown while checking the outstanding tasks
     */
    public static void waitForPendingTasks(final RestClient adminClient, final Predicate<String> taskFilter) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "/_cat/tasks");
                request.addParameter("detailed", "true");
                final Response response = adminClient.performRequest(request);
                /*
                 * Check to see if there are outstanding tasks; we exclude the list task itself, and any expected outstanding tasks using
                 * the specified task filter.
                 */
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try (
                        BufferedReader responseReader = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)
                        )
                    ) {
                        int activeTasks = 0;
                        String line;
                        final StringBuilder tasksListString = new StringBuilder();
                        while ((line = responseReader.readLine()) != null) {
                            final String taskName = line.split("\\s+")[0];
                            if (taskName.startsWith(ListTasksAction.NAME) || taskFilter.test(taskName)) {
                                continue;
                            }
                            activeTasks++;
                            tasksListString.append(line);
                            tasksListString.append('\n');
                        }
                        assertEquals(activeTasks + " active tasks found:\n" + tasksListString, 0, activeTasks);
                    }
                }
            } catch (final IOException e) {
                throw new AssertionError("error getting active tasks list", e);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    /**
     * Returns whether to preserve the state of the cluster upon completion of this test. Defaults to false. If true, overrides the value of
     * {@link #preserveIndicesUponCompletion()}, {@link #preserveTemplatesUponCompletion()}, {@link #preserveReposUponCompletion()},
     * and {@link #preserveSnapshotsUponCompletion()}.
     *
     * @return true if the state of the cluster should be preserved
     */
    protected boolean preserveClusterUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the indices created during this test on completion of this test.
     * Defaults to {@code false}. Override this method if indices should be preserved after the test,
     * with the assumption that some other process or test will clean up the indices afterward.
     * This is useful if the data directory and indices need to be preserved between test runs
     * (for example, when testing rolling upgrades).
     */
    protected boolean preserveIndicesUponCompletion() {
        return false;
    }

    /**
     * Controls whether or not to preserve templates upon completion of this test. The default implementation is to delete not preserve
     * templates.
     *
     * @return whether or not to preserve templates
     */
    protected boolean preserveTemplatesUponCompletion() {
        return false;
    }

    /**
     * Determines if data streams are preserved upon completion of this test. The default implementation wipes data streams.
     *
     * @return whether or not to preserve data streams
     */
    protected boolean preserveDataStreamsUponCompletion() {
        return false;
    }

    /**
     * Controls whether or not to preserve cluster settings upon completion of the test. The default implementation is to remove all cluster
     * settings.
     *
     * @return true if cluster settings should be preserved and otherwise false
     */
    protected boolean preserveClusterSettings() {
        return false;
    }

    /**
     * Returns whether to preserve the repositories on completion of this test.
     * Defaults to not preserving repos. See also
     * {@link #preserveSnapshotsUponCompletion()}.
     */
    protected boolean preserveReposUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the snapshots in repositories on completion of this
     * test. Defaults to not preserving snapshots. Only works for {@code fs} repositories.
     */
    protected boolean preserveSnapshotsUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve SLM Policies of this test. Defaults to not
     * preserving them. Only runs at all if xpack is installed on the cluster
     * being tested.
     */
    protected boolean preserveSLMPoliciesUponCompletion() {
        return false;
    }

    /**
     * Returns whether to wait to make absolutely certain that all snapshots
     * have been deleted.
     */
    protected boolean waitForAllSnapshotsWiped() {
        return false;
    }

    private void wipeCluster() throws Exception {

        SetOnce<Map<String, List<Map<?, ?>>>> inProgressSnapshots = new SetOnce<>();
        if (waitForAllSnapshotsWiped()) {
            AtomicReference<Map<String, List<Map<?, ?>>>> snapshots = new AtomicReference<>();
            try {
                // Repeatedly delete the snapshots until there aren't any
                assertBusy(() -> {
                    snapshots.set(wipeSnapshots());
                    assertThat(snapshots.get(), anEmptyMap());
                }, 2, TimeUnit.MINUTES);
                // At this point there should be no snaphots
                inProgressSnapshots.set(snapshots.get());
            } catch (AssertionError e) {
                // This will cause an error at the end of this method, but do the rest of the cleanup first
                inProgressSnapshots.set(snapshots.get());
            }
        } else {
            inProgressSnapshots.set(wipeSnapshots());
        }

        // wipe data streams before indices so that the backing indices for data streams are handled properly
        if (preserveDataStreamsUponCompletion() == false) {
            wipeDataStreams();
        }

        if (preserveIndicesUponCompletion() == false) {
            // wipe indices
            wipeAllIndices();
        }

        // wipe index templates
        if (preserveTemplatesUponCompletion() == false) {
            logger.debug("Clearing all templates");
            adminClient().performRequest(new Request("DELETE", "_template/*"));
            try {
                adminClient().performRequest(new Request("DELETE", "_index_template/*"));
                adminClient().performRequest(new Request("DELETE", "_component_template/*"));
            } catch (ResponseException e) {
                // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
            }
        }

        // wipe cluster settings
        if (preserveClusterSettings() == false) {
            wipeClusterSettings();
        }

        assertThat("Found in progress snapshots [" + inProgressSnapshots.get() + "].", inProgressSnapshots.get(), anEmptyMap());
    }

    protected static void wipeAllIndices() throws IOException {
        try {
            final Request deleteRequest = new Request("DELETE", "*");
            deleteRequest.addParameter("expand_wildcards", "open,closed,hidden");
            RequestOptions.Builder allowSystemIndexAccessWarningOptions = RequestOptions.DEFAULT.toBuilder();
            allowSystemIndexAccessWarningOptions.setWarningsHandler(warnings -> {
                if (warnings.size() == 0) {
                    return false;
                } else if (warnings.size() > 1) {
                    return true;
                }
                // We don't know exactly which indices we're cleaning up in advance, so just accept all system index access warnings.
                final String warning = warnings.get(0);
                final boolean isSystemIndexWarning = warning.contains("this request accesses system indices")
                    && warning.contains("but in a future major version, direct access to system indices will be prevented by default");
                return isSystemIndexWarning == false;
            });
            deleteRequest.setOptions(allowSystemIndexAccessWarningOptions);
            final Response response = adminClient().performRequest(deleteRequest);
            try (InputStream is = response.getEntity().getContent()) {
                assertTrue((boolean) XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), is, true).get("acknowledged"));
            }
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    protected static void wipeDataStreams() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
        } catch (ResponseException e) {
            // We hit a version of ES that doesn't serialize DeleteDataStreamAction.Request#wildcardExpressionsOriginallySpecified field or
            // that doesn't support data streams so it's safe to ignore
            int statusCode = e.getResponse().getStatusLine().getStatusCode();
            if (Set.of(404, 405, 500).contains(statusCode) == false) {
                throw e;
            }
        }
    }

    /**
     * Wipe fs snapshots we created one by one and all repositories so that the next test can create the repositories fresh and they'll
     * start empty. There isn't an API to delete all snapshots. There is an API to delete all snapshot repositories but that leaves all of
     * the snapshots intact in the repository.
     * @return Map of repository name to list of snapshots found in unfinished state
     */
    protected Map<String, List<Map<?, ?>>> wipeSnapshots() throws IOException {
        final Map<String, List<Map<?, ?>>> inProgressSnapshots = new HashMap<>();
        for (Map.Entry<String, ?> repo : entityAsMap(adminClient.performRequest(new Request("GET", "/_snapshot/_all"))).entrySet()) {
            String repoName = repo.getKey();
            Map<?, ?> repoSpec = (Map<?, ?>) repo.getValue();
            String repoType = (String) repoSpec.get("type");
            if (false == preserveSnapshotsUponCompletion() && repoType.equals("fs")) {
                // All other repo types we really don't have a chance of being able to iterate properly, sadly.
                Request listRequest = new Request("GET", "/_snapshot/" + repoName + "/_all");
                listRequest.addParameter("ignore_unavailable", "true");
                List<?> snapshots = (List<?>) entityAsMap(adminClient.performRequest(listRequest)).get("snapshots");
                for (Object snapshot : snapshots) {
                    Map<?, ?> snapshotInfo = (Map<?, ?>) snapshot;
                    String name = (String) snapshotInfo.get("snapshot");
                    if (SnapshotState.valueOf((String) snapshotInfo.get("state")).completed() == false) {
                        inProgressSnapshots.computeIfAbsent(repoName, key -> new ArrayList<>()).add(snapshotInfo);
                    }
                    logger.debug("wiping snapshot [{}/{}]", repoName, name);
                    adminClient().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + name));
                }
            }
            if (preserveReposUponCompletion() == false) {
                deleteRepository(repoName);
            }
        }
        return inProgressSnapshots;
    }

    protected void deleteRepository(String repoName) throws IOException {
        logger.debug("wiping snapshot repository [{}]", repoName);
        adminClient().performRequest(new Request("DELETE", "_snapshot/" + repoName));
    }

    /**
     * Remove any cluster settings.
     */
    private void wipeClusterSettings() throws IOException {
        Map<?, ?> getResponse = entityAsMap(adminClient().performRequest(new Request("GET", "/_cluster/settings")));

        boolean mustClear = false;
        XContentBuilder clearCommand = JsonXContent.contentBuilder();
        clearCommand.startObject();
        for (Map.Entry<?, ?> entry : getResponse.entrySet()) {
            String type = entry.getKey().toString();
            Map<?, ?> settings = (Map<?, ?>) entry.getValue();
            if (settings.isEmpty()) {
                continue;
            }
            mustClear = true;
            clearCommand.startObject(type);
            for (Object key : settings.keySet()) {
                clearCommand.field(key + ".*").nullValue();
            }
            clearCommand.endObject();
        }
        clearCommand.endObject();

        if (mustClear) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(clearCommand.toString());
            adminClient().performRequest(request);
        }
    }

    protected void refreshAllIndices() throws IOException {
        Request refreshRequest = new Request("POST", "/_refresh");
        refreshRequest.addParameter("expand_wildcards", "open,hidden");
        // Allow system index deprecation warnings
        final Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        requestOptions.setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            }
            boolean allSystemIndexWarnings = true;
            for (String warning : warnings) {
                if (!warning.startsWith("this request accesses system indices:")) {
                    allSystemIndexWarnings = false;
                    break;
                }
            }
            return !allSystemIndexWarnings;
        });
        refreshRequest.setOptions(requestOptions);
        client().performRequest(refreshRequest);
    }

    private static void deleteAllSLMPolicies() throws IOException {
        Map<String, Object> policies;

        try {
            Response response = adminClient().performRequest(new Request("GET", "/_slm/policy"));
            policies = entityAsMap(response);
        } catch (ResponseException e) {
            if (RestStatus.METHOD_NOT_ALLOWED.getStatus() == e.getResponse().getStatusLine().getStatusCode()
                || RestStatus.BAD_REQUEST.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                // If bad request returned, SLM is not enabled.
                return;
            }
            throw e;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            adminClient().performRequest(new Request("DELETE", "/_slm/policy/" + policyName));
        }
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    private void logIfThereAreRunningTasks() throws IOException {
        Set<String> runningTasks = runningTasks(adminClient().performRequest(new Request("GET", "/_tasks")));
        // Ignore the task list API - it doesn't count against us
        runningTasks.remove(ListTasksAction.NAME);
        runningTasks.remove(ListTasksAction.NAME + "[n]");
        if (runningTasks.isEmpty()) {
            return;
        }
        List<String> stillRunning = new ArrayList<>(runningTasks);
        sort(stillRunning);
        logger.info("There are still tasks running after this test that might break subsequent tests {}.", stillRunning);
        /*
         * This isn't a higher level log or outright failure because some of these tasks are run by the cluster in the background. If we
         * could determine that some tasks are run by the user we'd fail the tests if those tasks were running and ignore any background
         * tasks.
         */
    }

    /**
     * Waits for the cluster state updates to have been processed, so that no cluster
     * state updates are still in-progress when the next test starts.
     */
    private void waitForClusterStateUpdatesToFinish() throws Exception {
        assertBusy(() -> {
            try {
                Response response = adminClient().performRequest(new Request("GET", "/_cluster/pending_tasks"));
                List<?> tasks = (List<?>) entityAsMap(response).get("tasks");
                if (false == tasks.isEmpty()) {
                    StringBuilder message = new StringBuilder("there are still running tasks:");
                    for (Object task : tasks) {
                        message.append('\n').append(task.toString());
                    }
                    fail(message.toString());
                }
            } catch (IOException e) {
                fail("cannot get cluster's pending tasks: " + e.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder();
        if (System.getProperty("tests.rest.client_path_prefix") != null) {
            builder.put(CLIENT_PATH_PREFIX, System.getProperty("tests.rest.client_path_prefix"));
        }
        return builder.build();
    }

    /**
     * Returns the REST client settings used for admin actions like cleaning up after the test has completed.
     */
    protected Settings restAdminSettings() {
        return restClientSettings(); // default to the same client settings
    }

    /**
     * Get the list of hosts in the cluster.
     */
    protected final List<HttpHost> getClusterHosts() {
        return clusterHosts;
    }

    /**
     * Override this to switch to testing https.
     */
    protected String getProtocol() {
        return "http";
    }

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String keystorePath = settings.get(TRUSTSTORE_PATH);
        if (keystorePath != null) {
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(keystorePath);
            if (!Files.exists(path)) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                final String keyStoreType = keystorePath.endsWith(".p12") ? "PKCS12" : "jks";
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                final SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                builder.setHttpClientConfigCallback(httpClientBuilder -> {
                    final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setSslContext(sslcontext)
                        // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                        .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                            @Override
                            public TlsDetails create(final SSLEngine sslEngine) {
                                return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                            }
                        })
                        .build();

                    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                        .setTlsStrategy(tlsStrategy)
                        .build();

                    return httpClientBuilder.setConnectionManager(connectionManager);
                });
            } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(
            conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis())))
        );
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> runningTasks(Response response) throws IOException {
        Set<String> runningTasks = new HashSet<>();

        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(response).get("nodes");
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> nodeTasks = (Map<String, Object>) nodeInfo.get("tasks");
            for (Map.Entry<String, Object> taskAndName : nodeTasks.entrySet()) {
                Map<String, Object> task = (Map<String, Object>) taskAndName.getValue();
                runningTasks.add(task.get("action").toString());
            }
        }
        return runningTasks;
    }

    protected static void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    /**
     * checks that the specific index is green. we force a selection of an index as the tests share a cluster and often leave indices
     * in an non green state
     * @param index index to test for
     **/
    public static void ensureGreen(String index) throws IOException {
        ensureHealth(index, (request) -> {
            request.addParameter("wait_for_status", "green");
            request.addParameter("wait_for_no_relocating_shards", "true");
            request.addParameter("timeout", "70s");
            request.addParameter("level", "shards");
        });
    }

    protected static void ensureHealth(Consumer<Request> requestConsumer) throws IOException {
        ensureHealth("", requestConsumer);
    }

    protected static void ensureHealth(String index, Consumer<Request> requestConsumer) throws IOException {
        ensureHealth(client(), index, requestConsumer);
    }

    protected static void ensureHealth(RestClient client, String index, Consumer<Request> requestConsumer) throws IOException {
        Request request = new Request("GET", "/_cluster/health" + (index.trim().isEmpty() ? "" : "/" + index));
        requestConsumer.accept(request);
        try {
            client.performRequest(request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_REQUEST_TIMEOUT) {
                try {
                    final Response clusterStateResponse = client.performRequest(new Request("GET", "/_cluster/state?pretty"));
                    fail(
                        "timed out waiting for green state for index ["
                            + index
                            + "] "
                            + "cluster state ["
                            + EntityUtils.toString(clusterStateResponse.getEntity())
                            + "]"
                    );
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
            }
            throw e;
        }
    }

    /**
     * waits until all shard initialization is completed. This is a handy alternative to ensureGreen as it relates to all shards
     * in the cluster and doesn't require to know how many nodes/replica there are.
     */
    protected static void ensureNoInitializingShards() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        adminClient().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, null);
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        createIndex(name, settings, mapping, null);
    }

    protected static void createIndex(String name, Settings settings, String mapping, String aliases) throws IOException {
        Request request = new Request("PUT", "/" + name);
        String entity = "{\"settings\": " + Strings.toString(MediaTypeRegistry.JSON, settings);
        if (mapping != null) {
            entity += ",\"mappings\" : {" + mapping + "}";
        }
        if (aliases != null) {
            entity += ",\"aliases\": {" + aliases + "}";
        }
        entity += "}";
        if (settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        }
        request.setJsonEntity(entity);
        client().performRequest(request);
    }

    protected static void deleteIndex(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        client().performRequest(request);
    }

    protected static void updateIndexSettings(String index, Settings.Builder settings) throws IOException {
        updateIndexSettings(index, settings.build());
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(MediaTypeRegistry.JSON, settings));
        client().performRequest(request);
    }

    protected static void expectSoftDeletesWarning(Request request, String indexName) {
        final List<String> opensearchExpectedWarnings = Collections.singletonList(
            "Creating indices with soft-deletes disabled is deprecated and will be removed in future OpenSearch versions. "
                + "Please do not specify value for setting [index.soft_deletes.enabled] of index ["
                + indexName
                + "]."
        );
        final Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        requestOptions.setWarningsHandler(warnings -> warnings.equals(opensearchExpectedWarnings) == false);
        request.setOptions(requestOptions);
    }

    protected static Map<String, Object> getIndexSettings(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), is, true);
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getIndexSettingsAsMap(String index) throws IOException {
        Map<String, Object> indexSettings = getIndexSettings(index);
        return (Map<String, Object>) ((Map<String, Object>) indexSettings.get(index)).get("settings");
    }

    protected static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static void closeIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_close"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static void openIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_open"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static boolean aliasExists(String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static boolean aliasExists(String index, String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index + "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getAlias(final String index, final String alias) throws IOException {
        String endpoint = "/_alias";
        if (false == Strings.isEmpty(index)) {
            endpoint = index + endpoint;
        }
        if (false == Strings.isEmpty(alias)) {
            endpoint = endpoint + "/" + alias;
        }
        Map<String, Object> getAliasResponse = getAsMap(endpoint);
        return (Map<String, Object>) XContentMapValues.extractValue(index + ".aliases." + alias, getAliasResponse);
    }

    protected static Map<String, Object> getAsMap(final String endpoint) throws IOException {
        Response response = client().performRequest(new Request("GET", endpoint));
        return responseAsMap(response);
    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        MediaType entityContentType = MediaType.fromMediaType(response.getEntity().getContentType());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(
            entityContentType.xContent(),
            response.getEntity().getContent(),
            false
        );
        assertNotNull(responseEntity);
        return responseEntity;
    }

    protected static void registerRepository(String repository, String type, boolean verify, Settings settings) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository);
        request.addParameter("verify", Boolean.toString(verify));
        request.setJsonEntity(Strings.toString(MediaTypeRegistry.JSON, new PutRepositoryRequest(repository).type(type).settings(settings)));

        final Response response = client().performRequest(request);
        assertAcked("Failed to create repository [" + repository + "] of type [" + type + "]: " + response, response);
    }

    protected static void createSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void restoreSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot + "/_restore");
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + snapshot + "] from repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    @SuppressWarnings("unchecked")
    private static void assertAcked(String message, Response response) throws IOException {
        final int responseStatusCode = response.getStatusLine().getStatusCode();
        assertThat(
            message + ": expecting response code [200] but got [" + responseStatusCode + ']',
            responseStatusCode,
            equalTo(RestStatus.OK.getStatus())
        );
        final Map<String, Object> responseAsMap = responseAsMap(response);
        Boolean acknowledged = (Boolean) XContentMapValues.extractValue(responseAsMap, "acknowledged");
        assertThat(message + ": response is not acknowledged", acknowledged, equalTo(Boolean.TRUE));
    }

    public void flush(String index, boolean force) throws IOException {
        logger.info("flushing index {} force={}", index, force);
        final Request flushRequest = new Request("POST", "/" + index + "/_flush");
        flushRequest.addParameter("force", Boolean.toString(force));
        flushRequest.addParameter("wait_if_ongoing", "true");
        assertOK(client().performRequest(flushRequest));
    }

    /**
     * Asserts that replicas on nodes satisfying the {@code targetNode} should have perform operation-based recoveries.
     */
    public void assertNoFileBasedRecovery(String indexName, Predicate<String> targetNode) throws IOException {
        Map<String, Object> recoveries = entityAsMap(client().performRequest(new Request("GET", indexName + "/_recovery?detailed=true")));
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> shards = (List<Map<String, ?>>) XContentMapValues.extractValue(indexName + ".shards", recoveries);
        assertNotNull(shards);
        boolean foundReplica = false;
        logger.info("index {} recovery stats {}", indexName, shards);
        for (Map<String, ?> shard : shards) {
            if (shard.get("primary") == Boolean.FALSE && targetNode.test((String) XContentMapValues.extractValue("target.name", shard))) {
                List<?> details = (List<?>) XContentMapValues.extractValue("index.files.details", shard);
                // once detailed recoveries works, remove this if.
                if (details == null) {
                    long totalFiles = ((Number) XContentMapValues.extractValue("index.files.total", shard)).longValue();
                    long reusedFiles = ((Number) XContentMapValues.extractValue("index.files.reused", shard)).longValue();
                    logger.info("total [{}] reused [{}]", totalFiles, reusedFiles);
                    assertThat("must reuse all files, recoveries [" + recoveries + "]", totalFiles, equalTo(reusedFiles));
                } else {
                    assertNotNull(details);
                    assertThat(details, Matchers.empty());
                }
                foundReplica = true;
            }
        }
        assertTrue("must find replica", foundReplica);
    }

    /**
     * Asserts that we do not retain any extra translog for the given index (i.e., turn off the translog retention)
     */
    public void assertEmptyTranslog(String index) throws Exception {
        Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
        assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.operations", stats), equalTo(0));
    }

    /**
     * Peer recovery retention leases are renewed and synced to replicas periodically (every 30 seconds). This ensures
     * that we have renewed every PRRL to the global checkpoint of the corresponding copy and properly synced to all copies.
     */
    public void ensurePeerRecoveryRetentionLeasesRenewedAndSynced(String index) throws Exception {
        assertBusy(() -> {
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            @SuppressWarnings("unchecked")
            Map<String, List<Map<String, ?>>> shards = (Map<String, List<Map<String, ?>>>) XContentMapValues.extractValue(
                "indices." + index + ".shards",
                stats
            );
            for (List<Map<String, ?>> shard : shards.values()) {
                for (Map<String, ?> copy : shard) {
                    Integer globalCheckpoint = (Integer) XContentMapValues.extractValue("seq_no.global_checkpoint", copy);
                    assertNotNull(globalCheckpoint);
                    assertThat(XContentMapValues.extractValue("seq_no.max_seq_no", copy), equalTo(globalCheckpoint));
                    @SuppressWarnings("unchecked")
                    List<Map<String, ?>> retentionLeases = (List<Map<String, ?>>) XContentMapValues.extractValue(
                        "retention_leases.leases",
                        copy
                    );
                    assertNotNull(retentionLeases);
                    for (Map<String, ?> retentionLease : retentionLeases) {
                        if (((String) retentionLease.get("id")).startsWith("peer_recovery/")) {
                            assertThat(retentionLease.get("retaining_seq_no"), equalTo(globalCheckpoint + 1));
                        }
                    }
                    List<String> existingLeaseIds = retentionLeases.stream()
                        .map(lease -> (String) lease.get("id"))
                        .collect(Collectors.toList());
                    List<String> expectedLeaseIds = shard.stream()
                        .map(shr -> (String) XContentMapValues.extractValue("routing.node", shr))
                        .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId)
                        .collect(Collectors.toList());
                    assertThat("not every active copy has established its PPRL", expectedLeaseIds, everyItem(in(existingLeaseIds)));
                }
            }
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Returns the minimum node version among all nodes of the cluster
     */
    protected static Version minimumNodeVersion() throws IOException {
        final Request request = new Request("GET", "_nodes");
        request.addParameter("filter_path", "nodes.*.version");

        final Response response = adminClient().performRequest(request);
        final Map<String, Object> nodes = ObjectPath.createFromResponse(response).evaluate("nodes");

        Version minVersion = null;
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            @SuppressWarnings("unchecked")
            Version nodeVersion = Version.fromString((String) ((Map<String, Object>) node.getValue()).get("version"));
            if (minVersion == null || minVersion.after(nodeVersion)) {
                minVersion = nodeVersion;
            }
        }
        assertNotNull(minVersion);
        return minVersion;
    }

    protected void syncedFlush(String indexName, boolean retryOnConflict) throws Exception {
        final Request request = new Request("POST", indexName + "/_flush/synced");
        final Builder options = RequestOptions.DEFAULT.toBuilder();
        // 8.0 kept in warning message for legacy purposes TODO: changge to 3.0
        final List<String> warningMessage = Arrays.asList(
            "Synced flush is deprecated and will be removed in 3.0. Use flush at _/flush or /{index}/_flush instead."
        );
        final List<String> expectedWarnings = Arrays.asList(
            "Synced flush was removed and a normal flush was performed instead. This transition will be removed in a future version."
        );
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_2_0_0))) {
            options.setWarningsHandler(warnings -> warnings.isEmpty() == false && warnings.equals(expectedWarnings) == false);
        } else {
            options.setWarningsHandler(
                warnings -> warnings.isEmpty() == false
                    && warnings.equals(expectedWarnings) == false
                    && warnings.equals(warningMessage) == false
            );
        }
        request.setOptions(options);
        // We have to spin synced-flush requests here because we fire the global checkpoint sync for the last write operation.
        // A synced-flush request considers the global checkpoint sync as an going operation because it acquires a shard permit.
        assertBusy(() -> {
            try {
                Response resp = client().performRequest(request);
                if (retryOnConflict) {
                    Map<String, Object> result = ObjectPath.createFromResponse(resp).evaluate("_shards");
                    assertThat(result.get("failed"), equalTo(0));
                }
            } catch (ResponseException ex) {
                assertThat(ex.getResponse().getStatusLine(), equalTo(HttpStatus.SC_CONFLICT));
                if (retryOnConflict) {
                    throw new AssertionError(ex); // cause assert busy to retry
                }
            }
        });
        // ensure the global checkpoint is synced; otherwise we might trim the commit with syncId
        ensureGlobalCheckpointSynced(indexName);
    }

    @SuppressWarnings("unchecked")
    private void ensureGlobalCheckpointSynced(String index) throws Exception {
        assertBusy(() -> {
            Map<?, ?> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            List<Map<?, ?>> shardStats = (List<Map<?, ?>>) XContentMapValues.extractValue("indices." + index + ".shards.0", stats);
            shardStats.stream()
                .map(shard -> (Map<?, ?>) XContentMapValues.extractValue("seq_no", shard))
                .filter(Objects::nonNull)
                .forEach(seqNoStat -> {
                    long globalCheckpoint = ((Number) XContentMapValues.extractValue("global_checkpoint", seqNoStat)).longValue();
                    long localCheckpoint = ((Number) XContentMapValues.extractValue("local_checkpoint", seqNoStat)).longValue();
                    long maxSeqNo = ((Number) XContentMapValues.extractValue("max_seq_no", seqNoStat)).longValue();
                    assertThat(shardStats.toString(), localCheckpoint, equalTo(maxSeqNo));
                    assertThat(shardStats.toString(), globalCheckpoint, equalTo(maxSeqNo));
                });
        }, 60, TimeUnit.SECONDS);
    }

    static final Pattern CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile(
        "^index \\[(.+)\\] matches multiple legacy " + "templates \\[(.+)\\], composable templates will only match a single template$"
    );

    static final Pattern PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES = Pattern.compile(
        "^index template \\[(.+)\\] has index patterns "
            + "\\[(.+)\\] matching patterns from existing older templates \\[(.+)\\] with patterns \\((.+)\\); this template \\[(.+)\\] will "
            + "take precedence during new index creation$"
    );

    protected static void useIgnoreMultipleMatchingTemplatesWarningsHandler(Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(warnings -> {
            if (warnings.size() > 0) {
                boolean matches = warnings.stream()
                    .anyMatch(
                        message -> CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches()
                            || PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(message).matches()
                    );
                return matches == false;
            } else {
                return false;
            }
        });
        request.setOptions(options);
    }
}
