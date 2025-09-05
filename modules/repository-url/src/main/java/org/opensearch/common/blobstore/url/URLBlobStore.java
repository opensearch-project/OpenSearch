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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

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
        this.path = path;
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
        } catch (MalformedURLException | URISyntaxException ex) {
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
    private URL buildPath(BlobPath path) throws MalformedURLException, URISyntaxException {
        String[] paths = path.toArray();
        if (paths.length == 0) {
            return path();
        }
        var uri = this.path.toURI();
        for (String pathElement : paths) {
            uri = uri.resolve(pathElement + "/");
        }
        return uri.toURL();
    }
}
