/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.opensearch.gradle.testclusters.OpenSearchCluster

import javax.net.ssl.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.GeneralSecurityException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import java.util.stream.Collectors

class WaitForClusterSetupTask extends DefaultTask {

    @Input
    OpenSearchCluster cluster

    @Input
    boolean securityEnabled = false

    @Input
    String username = System.getProperty("user", "admin")

    @Input
    String password = System.getProperty("password", "admin")

    @Input
    int timeoutMs = 180000

    @TaskAction
    void setupCluster() {
        cluster.@waitConditions.clear()

        // Write unicast hosts
        String unicastUris = cluster.nodes.stream().flatMap { node ->
            node.getAllTransportPortURI().stream()
        }.collect(Collectors.joining("\n"))

        cluster.nodes.forEach { node ->
            try {
                Files.write(node.getConfigDir().resolve("unicast_hosts.txt"),
                           unicastUris.getBytes(StandardCharsets.UTF_8))
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("Failed to write configuration files", e)
            }
        }

        // Add wait condition
        Predicate pred = {
            String protocol = securityEnabled ? "https" : "http"
            String host = System.getProperty("tests.cluster", cluster.getFirstNode().getHttpSocketURI())
            WaitForClusterYellow wait = new WaitForClusterYellow(protocol, host, cluster.nodes.size())
            wait.setUsername(username)
            wait.setPassword(password)
            return wait.wait(timeoutMs)
        }

        cluster.@waitConditions.put("cluster health yellow", pred)
        cluster.waitForAllConditions()
    }
}

class WaitForClusterYellow {
    private URL url
    private String username
    private String password
    Set<Integer> validResponseCodes = Collections.singleton(200)

    WaitForClusterYellow(String protocol, String host, int numberOfNodes) throws MalformedURLException {
        this(new URL(protocol + "://" + host + "/_cluster/health?wait_for_nodes=>=" + numberOfNodes + "&wait_for_status=yellow"))
    }

    WaitForClusterYellow(URL url) {
        this.url = url
    }

    boolean wait(int durationInMs) throws GeneralSecurityException, InterruptedException, IOException {
        final long waitUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationInMs)
        final long sleep = 100

        IOException failure = null
        while (true) {

            try {
                checkResource()
                return true
            } catch (IOException e) {
                failure = e
            }
            if (System.nanoTime() < waitUntil) {
                Thread.sleep(sleep)
            } else {
                throw failure
            }
        }
    }

    void setUsername(String username) {
        this.username = username
    }

    void setPassword(String password) {
        this.password = password
    }

    void checkResource() throws IOException {
        final HttpURLConnection connection = buildConnection()
        connection.connect()
        final Integer response = connection.getResponseCode()
        if (validResponseCodes.contains(response)) {
            return
        } else {
            throw new IOException(response + " " + connection.getResponseMessage())
        }
    }

    HttpURLConnection buildConnection() throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) this.@url.openConnection()

        if (connection instanceof HttpsURLConnection) {
            TrustManager[] trustAllCerts = [new X509TrustManager() {
                X509Certificate[] getAcceptedIssuers() { return null }
                void checkClientTrusted(X509Certificate[] certs, String authType) {}
                void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }] as TrustManager[]

            SSLContext sc = SSLContext.getInstance("SSL")
            sc.init(null, trustAllCerts, new java.security.SecureRandom())
            connection.setSSLSocketFactory(sc.getSocketFactory())
            connection.setHostnameVerifier({ hostname, session -> true } as HostnameVerifier)
        }

        configureBasicAuth(connection)
        connection.setRequestMethod("GET")
        return connection
    }

    void configureBasicAuth(HttpURLConnection connection) {
        // only configure security if https is enabled
        if ("https".equals(url.getProtocol())) {
            if (username != null) {
                if (password == null) {
                    throw new IllegalStateException("Basic Auth user [" + username + "] has been set, but no password has been configured")
                }
                connection.setRequestProperty(
                    "Authorization",
                    "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8))
                )
            }
        }
    }
}
