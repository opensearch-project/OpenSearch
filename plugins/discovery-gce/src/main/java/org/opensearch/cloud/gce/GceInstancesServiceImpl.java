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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cloud.gce;

import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.cloud.gce.util.Access;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.gce.RetryHttpInitializerWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class GceInstancesServiceImpl implements GceInstancesService {

    private static final Logger logger = LogManager.getLogger(GceInstancesServiceImpl.class);

    // all settings just used for testing - not registered by default
    public static final Setting<Boolean> GCE_VALIDATE_CERTIFICATES = Setting.boolSetting(
        "cloud.gce.validate_certificates",
        true,
        Property.NodeScope
    );
    public static final Setting<String> GCE_ROOT_URL = new Setting<>(
        "cloud.gce.root_url",
        "https://www.googleapis.com",
        Function.identity(),
        Property.NodeScope
    );

    private static final Supplier<Boolean> inFipsMode = () -> {
        try {
            // Equivalent to: boolean approvedOnly = CryptoServicesRegistrar.isInApprovedOnlyMode()
            var registrarClass = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar");
            var isApprovedOnlyMethod = registrarClass.getMethod("isInApprovedOnlyMode");
            return (Boolean) isApprovedOnlyMethod.invoke(null);
        } catch (ReflectiveOperationException e) {
            return false;
        }
    };

    private final String project;
    private final List<String> zones;

    @Override
    public Collection<Instance> instances() {
        logger.debug("get instances for project [{}], zones [{}]", project, zones);
        final List<Instance> instances = zones.stream().map((zoneId) -> {
            try {
                // hack around code messiness in GCE code
                // TODO: get this fixed
                InstanceList instanceList = Access.doPrivilegedIOException(() -> {
                    Compute.Instances.List list = client().instances().list(project, zoneId);
                    return list.execute();
                });
                // assist type inference
                return instanceList.isEmpty() || instanceList.getItems() == null
                    ? Collections.<Instance>emptyList()
                    : instanceList.getItems();
            } catch (IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Problem fetching instance list for zone {}", zoneId), e);
                logger.debug("Full exception:", e);
                // assist type inference
                return Collections.<Instance>emptyList();
            }
        }).reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        });

        if (instances.isEmpty()) {
            logger.warn("disabling GCE discovery. Can not get list of nodes");
        }

        return instances;
    }

    private final Settings settings;
    private Compute client;
    private TimeValue refreshInterval = null;
    private long lastRefresh;

    /** Global instance of the HTTP transport. */
    private HttpTransport gceHttpTransport;

    /** Global instance of the JSON factory. */
    private JsonFactory gceJsonFactory;

    private final boolean validateCerts;

    public GceInstancesServiceImpl(Settings settings) {
        this.settings = settings;
        this.validateCerts = GCE_VALIDATE_CERTIFICATES.get(settings);
        this.project = resolveProject();
        this.zones = resolveZones();
    }

    private String resolveProject() {
        if (PROJECT_SETTING.exists(settings)) {
            return PROJECT_SETTING.get(settings);
        }

        try {
            // this code is based on a private GCE method: {@link com.google.cloud.ServiceOptions#getAppEngineProjectIdFromMetadataServer()}
            return getAppEngineValueFromMetadataServer("/computeMetadata/v1/project/project-id");
        } catch (Exception e) {
            logger.warn("unable to resolve project from metadata server for GCE discovery service", e);
        }
        return null;
    }

    private List<String> resolveZones() {
        if (ZONE_SETTING.exists(settings)) {
            return ZONE_SETTING.get(settings);
        }

        try {
            final String defaultZone = getAppEngineValueFromMetadataServer(
                "/computeMetadata/v1/project/attributes/google-compute-default-zone"
            );
            return Collections.singletonList(defaultZone);
        } catch (Exception e) {
            logger.warn("unable to resolve default zone from metadata server for GCE discovery service", e);
        }
        return null;
    }

    String getAppEngineValueFromMetadataServer(String serviceURL) throws GeneralSecurityException, IOException {
        String metadata = GceMetadataService.GCE_HOST.get(settings);
        GenericUrl url = Access.doPrivileged(() -> new GenericUrl(metadata + serviceURL));

        HttpTransport httpTransport = getGceHttpTransport();
        HttpRequestFactory requestFactory = httpTransport.createRequestFactory();
        HttpRequest request = requestFactory.buildGetRequest(url)
            .setConnectTimeout(500)
            .setReadTimeout(500)
            .setHeaders(new HttpHeaders().set("Metadata-Flavor", "Google"));
        HttpResponse response = Access.doPrivilegedIOException(() -> request.execute());
        return headerContainsMetadataFlavor(response) ? response.parseAsString() : null;
    }

    private static boolean headerContainsMetadataFlavor(HttpResponse response) {
        // com.google.cloud.ServiceOptions#headerContainsMetadataFlavor(HttpResponse)}
        String metadataFlavorValue = response.getHeaders().getFirstHeaderStringValue("Metadata-Flavor");
        return "Google".equals(metadataFlavorValue);
    }

    protected synchronized HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (gceHttpTransport == null) {
            if (validateCerts) {
                if (inFipsMode.get()) {
                    KeyStore trustStore = KeyStore.getInstance("BCFKS");
                    try (InputStream in = getClass().getResourceAsStream("/google.bcfks")) {
                        trustStore.load(in, "notasecret".toCharArray());
                    }
                    gceHttpTransport = new NetHttpTransport.Builder().trustCertificates(trustStore).build();
                } else {
                    gceHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
                    ;
                }
            } else {
                // this is only used for testing - alternative we could use the defaul keystore but this requires special configs too..
                gceHttpTransport = new NetHttpTransport.Builder().doNotValidateCertificate().build();
            }
        }
        return gceHttpTransport;
    }

    public synchronized Compute client() {
        if (refreshInterval != null && refreshInterval.millis() != 0) {
            if (client != null && (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve client");
                return client;
            }
            lastRefresh = System.currentTimeMillis();
        }

        try {
            gceJsonFactory = new JacksonFactory();

            logger.info("starting GCE discovery service");
            // Forcing Google Token API URL as set in GCE SDK to
            // http://metadata/computeMetadata/v1/instance/service-accounts/default/token
            // See https://developers.google.com/compute/docs/metadata#metadataserver
            String tokenServerEncodedUrl = GceMetadataService.GCE_HOST.get(settings)
                + "/computeMetadata/v1/instance/service-accounts/default/token";
            ComputeCredential credential = new ComputeCredential.Builder(getGceHttpTransport(), gceJsonFactory).setTokenServerEncodedUrl(
                tokenServerEncodedUrl
            ).build();

            // hack around code messiness in GCE code
            // TODO: get this fixed
            Access.doPrivilegedIOException(credential::refreshToken);

            logger.debug("token [{}] will expire in [{}] s", credential.getAccessToken(), credential.getExpiresInSeconds());
            if (credential.getExpiresInSeconds() != null) {
                refreshInterval = TimeValue.timeValueSeconds(credential.getExpiresInSeconds() - 1);
            }

            Compute.Builder builder = new Compute.Builder(getGceHttpTransport(), gceJsonFactory, null).setApplicationName(VERSION)
                .setRootUrl(GCE_ROOT_URL.get(settings));

            if (RETRY_SETTING.exists(settings)) {
                TimeValue maxWait = MAX_WAIT_SETTING.get(settings);
                RetryHttpInitializerWrapper retryHttpInitializerWrapper;

                if (maxWait.getMillis() > 0) {
                    retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential, maxWait);
                } else {
                    retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential);
                }
                builder.setHttpRequestInitializer(retryHttpInitializerWrapper);

            } else {
                builder.setHttpRequestInitializer(credential);
            }

            this.client = builder.build();
        } catch (Exception e) {
            logger.warn("unable to start GCE discovery service", e);
            throw new IllegalArgumentException("unable to start GCE discovery service", e);
        }

        return this.client;
    }

    @Override
    public String projectId() {
        return project;
    }

    @Override
    public List<String> zones() {
        return zones;
    }

    @Override
    public void close() throws IOException {
        if (gceHttpTransport != null) {
            gceHttpTransport.shutdown();
        }
    }
}
