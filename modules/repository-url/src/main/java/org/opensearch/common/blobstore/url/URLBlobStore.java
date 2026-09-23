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

package org.opensearch.common.blobstore.url;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

/**
 * Read-only URL-based blob store
 */
public class URLBlobStore implements BlobStore {

    private final URL path;

    private final int bufferSizeInBytes;

    /**
     * Constructs new read-only URL-based blob store
     * <p>
     * The following settings are supported
     * <dl>
     * <dt>buffer_size</dt>
     * <dd>- size of the read buffer, defaults to 100KB</dd>
     * </dl>
     *
     * @param settings settings
     * @param path     base URL
     */
    public URLBlobStore(Settings settings, URL path) {
        try {
            this.path = normalizeRootURL(path);
        } catch (IOException e) {
            throw new BlobStoreException("malformed URL " + path, e);
        }
        this.bufferSizeInBytes = (int) settings.getAsBytesSize("repositories.uri.buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB))
            .getBytes();
    }

    @Override
    public String toString() {
        return path.toString();
    }

    /**
     * Returns base URL
     *
     * @return base URL
     */
    public URL path() {
        return path;
    }

    /**
     * Returns read buffer size
     *
     * @return read buffer size
     */
    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new URLBlobContainer(this, path, buildPath(path));
        } catch (IOException ex) {
            throw new BlobStoreException("malformed URL " + path, ex);
        }
    }

    @Override
    public void close() {
        // nothing to do here...
    }

    /**
     * Builds URL using base URL and specified path
     *
     * @param path relative path
     * @return Base URL + path
     */
    private URL buildPath(BlobPath path) throws IOException {
        URL resolvedPath = this.path;
        for (String pathElement : path.toArray()) {
            resolvedPath = resolve(resolvedPath, pathElement + "/");
        }
        return resolvedPath;
    }

    /**
     * Resolves a relative path while keeping the result within both the repository root and the current container.
     */
    URL resolve(URL basePath, String relativePath) throws IOException {
        final URI relativeURI = parseRelativePath(relativePath);
        final URL resolvedPath;
        try {
            final URI baseURI = basePath.toURI();
            resolvedPath = baseURI.isOpaque()
                ? appendToPath(baseURI, relativeURI.toASCIIString())
                : baseURI.resolve(relativeURI).normalize().toURL();
        } catch (MalformedURLException | URISyntaxException | IllegalArgumentException e) {
            throw invalidPath(relativePath, e);
        }

        if (isWithin(this.path, resolvedPath) == false || isWithin(basePath, resolvedPath) == false) {
            throw invalidPath(relativePath, null);
        }
        return resolvedPath;
    }

    private static URI parseRelativePath(String path) throws IOException {
        final URI relativeURI;
        try {
            relativeURI = new URI(path);
        } catch (URISyntaxException e) {
            throw invalidPath(path, e);
        }

        final String rawPath = relativeURI.getRawPath();
        final String decodedPath = relativeURI.getPath();
        if (relativeURI.isAbsolute()
            || relativeURI.getRawAuthority() != null
            || relativeURI.getRawQuery() != null
            || relativeURI.getRawFragment() != null
            || rawPath == null
            || decodedPath == null
            || rawPath.startsWith("/")
            || decodedPath.startsWith("/")
            || rawPath.indexOf('\\') >= 0
            || decodedPath.indexOf('\\') >= 0
            || decodedPath.indexOf('%') >= 0
            || count(decodedPath, '/') != count(rawPath, '/')
            || containsControlCharacter(decodedPath)) {
            throw invalidPath(path, null);
        }

        for (String pathElement : decodedPath.split("/", -1)) {
            if (pathElement.equals(".") || pathElement.equals("..")) {
                throw invalidPath(path, null);
            }
        }

        if (rawPath.indexOf('%') >= 0) {
            try {
                final String canonicalPath = new URI(null, null, decodedPath, null).toASCIIString();
                if (normalizePercentEncoding(relativeURI.toASCIIString()).equals(canonicalPath) == false) {
                    throw invalidPath(path, null);
                }
            } catch (URISyntaxException e) {
                throw invalidPath(path, e);
            }
        }
        return relativeURI;
    }

    private static boolean isWithin(URL basePath, URL resolvedPath) {
        final URI baseURI;
        final URI resolvedURI;
        try {
            baseURI = basePath.toURI();
            resolvedURI = resolvedPath.toURI();
        } catch (URISyntaxException e) {
            return false;
        }

        if (Objects.equals(baseURI.getScheme(), resolvedURI.getScheme()) == false
            || Objects.equals(baseURI.getRawAuthority(), resolvedURI.getRawAuthority()) == false
            || baseURI.isOpaque() != resolvedURI.isOpaque()) {
            return false;
        }

        final String base = baseURI.isOpaque() ? baseURI.getRawSchemeSpecificPart() : baseURI.getRawPath();
        final String resolved = resolvedURI.isOpaque() ? resolvedURI.getRawSchemeSpecificPart() : resolvedURI.getRawPath();
        if (base == null || resolved == null) {
            return false;
        }
        final String basePrefix = base.endsWith("/") ? base : base + "/";
        return resolved.equals(base) || resolved.startsWith(basePrefix);
    }

    private static URL normalizeRootURL(URL path) throws IOException {
        final URI normalizedURI;
        try {
            normalizedURI = path.toURI().normalize();
        } catch (URISyntaxException e) {
            throw new IOException("malformed URL [" + path + "]", e);
        }

        final URL normalizedURL = normalizedURI.toURL();
        final String normalizedPath = normalizedURI.isOpaque() ? normalizedURI.getRawSchemeSpecificPart() : normalizedURI.getRawPath();
        if (normalizedPath == null) {
            throw new IOException("malformed URL [" + path + "]");
        }
        if (normalizedPath.endsWith("/")) {
            return normalizedURL;
        }

        if (normalizedURI.isOpaque()) {
            return appendToPath(normalizedURI, "/");
        }

        final String rawPath = normalizedURI.getRawPath();
        final StringBuilder normalized = new StringBuilder().append(normalizedURI.getScheme()).append(':');
        if (normalizedURI.getRawAuthority() != null) {
            normalized.append("//").append(normalizedURI.getRawAuthority());
        }
        if (rawPath == null || rawPath.isEmpty()) {
            normalized.append('/');
        } else {
            normalized.append(rawPath).append('/');
        }
        if (normalizedURI.getRawQuery() != null) {
            normalized.append('?').append(normalizedURI.getRawQuery());
        }
        if (normalizedURI.getRawFragment() != null) {
            normalized.append('#').append(normalizedURI.getRawFragment());
        }
        return URI.create(normalized.toString()).toURL();
    }

    private static URL appendToPath(URI basePath, String suffix) throws MalformedURLException {
        final String externalForm = basePath.toString();
        final int pathEnd = basePath.getRawFragment() == null ? externalForm.length() : externalForm.lastIndexOf('#');
        try {
            return new URI(externalForm.substring(0, pathEnd) + suffix + externalForm.substring(pathEnd)).toURL();
        } catch (URISyntaxException e) {
            final MalformedURLException malformedURLException = new MalformedURLException("malformed URL [" + basePath + "]");
            malformedURLException.initCause(e);
            throw malformedURLException;
        }
    }

    private static int count(String value, char character) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == character) {
                count++;
            }
        }
        return count;
    }

    private static boolean containsControlCharacter(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static String normalizePercentEncoding(String value) {
        final StringBuilder normalized = new StringBuilder(value);
        for (int i = 0; i < normalized.length(); i++) {
            if (normalized.charAt(i) == '%') {
                normalized.setCharAt(i + 1, Character.toUpperCase(normalized.charAt(i + 1)));
                normalized.setCharAt(i + 2, Character.toUpperCase(normalized.charAt(i + 2)));
                i += 2;
            }
        }
        return normalized.toString();
    }

    private static IOException invalidPath(String path, Exception cause) {
        return new IOException("invalid URL path [" + path + "]", cause);
    }
}
