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

package org.opensearch.repositories.url;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.url.URLBlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.URIPattern;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Read-only URL-based implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code url}</dt><dd>URL to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * </dl>
 */
public class URLRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(URLRepository.class);

    public static final String TYPE = "url";

    public static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING = Setting.listSetting(
        "repositories.url.supported_protocols",
        Arrays.asList("http", "https", "ftp", "file", "jar"),
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<List<URIPattern>> ALLOWED_URLS_SETTING = Setting.listSetting(
        "repositories.url.allowed_urls",
        Collections.emptyList(),
        URIPattern::new,
        Property.NodeScope
    );

    public static final Setting<URL> URL_SETTING = new Setting<>("url", "http://?", URLRepository::parseURL, Property.NodeScope);
    public static final Setting<URL> REPOSITORIES_URL_SETTING = new Setting<>(
        "repositories.url.url",
        (s) -> s.get("repositories.uri.url", "http://?"),
        URLRepository::parseURL,
        Property.NodeScope
    );

    private final List<String> supportedProtocols;

    private final URIPattern[] urlAllowList;

    private final Environment environment;

    private final BlobPath basePath;

    private final URL url;

    /**
     * Constructs a read-only URL-based repository
     */
    public URLRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);

        if (URL_SETTING.exists(metadata.settings()) == false && REPOSITORIES_URL_SETTING.exists(environment.settings()) == false) {
            throw new RepositoryException(metadata.name(), "missing url");
        }
        this.environment = environment;
        supportedProtocols = SUPPORTED_PROTOCOLS_SETTING.get(environment.settings());
        urlAllowList = ALLOWED_URLS_SETTING.get(environment.settings()).toArray(new URIPattern[] {});
        basePath = BlobPath.cleanPath();
        url = URL_SETTING.exists(metadata.settings())
            ? URL_SETTING.get(metadata.settings())
            : REPOSITORIES_URL_SETTING.get(environment.settings());
    }

    @Override
    protected BlobStore createBlobStore() {
        URL normalizedURL = checkURL(url);
        return new URLBlobStore(environment.settings(), normalizedURL);
    }

    // only use for testing
    @Override
    protected BlobContainer blobContainer() {
        return super.blobContainer();
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Makes sure that the url is white listed or if it points to the local file system it matches one on of the root path in path.repo
     */
    private URL checkURL(URL url) {
        String protocol = url.getProtocol();
        if (protocol == null) {
            throw new RepositoryException(getMetadata().name(), "unknown url protocol from URL [" + url + "]");
        }
        for (String supportedProtocol : supportedProtocols) {
            if (supportedProtocol.equals(protocol)) {
                try {
                    if (URIPattern.match(urlAllowList, url.toURI())) {
                        // URL matches white list - no additional processing is needed
                        return url;
                    }
                } catch (URISyntaxException ex) {
                    logger.warn("cannot parse the specified url [{}]", url);
                    throw new RepositoryException(getMetadata().name(), "cannot parse the specified url [" + url + "]");
                }
                // We didn't match white list - try to resolve against path.repo
                URL normalizedUrl = environment.resolveRepoURL(url);
                if (normalizedUrl == null) {
                    String logMessage = "The specified url [{}] doesn't start with any repository paths specified by the "
                        + "path.repo setting or by {} setting: [{}] ";
                    logger.warn(logMessage, url, ALLOWED_URLS_SETTING.getKey(), environment.repoFiles());
                    String exceptionMessage = "file url ["
                        + url
                        + "] doesn't match any of the locations specified by path.repo or "
                        + ALLOWED_URLS_SETTING.getKey();
                    throw new RepositoryException(getMetadata().name(), exceptionMessage);
                }
                return normalizedUrl;
            }
        }
        throw new RepositoryException(getMetadata().name(), "unsupported url protocol [" + protocol + "] from URL [" + url + "]");
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    private static URL parseURL(String s) {
        try {
            return URI.create(s).toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Unable to parse URL repository setting", e);
        }
    }
}
