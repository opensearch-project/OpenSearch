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

import com.sun.net.httpserver.HttpServer;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

@SuppressForbidden(reason = "use http server")
public class URLBlobStoreTests extends OpenSearchTestCase {

    private static HttpServer httpServer;
    private static String blobName;
    private static byte[] message = new byte[512];
    private URLBlobStore urlBlobStore;

    @BeforeClass
    public static void startHttp() throws Exception {
        for (int i = 0; i < message.length; ++i) {
            message[i] = randomByte();
        }
        blobName = randomAlphaOfLength(8);

        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 6001), 0);

        createContext("/indices/" + blobName);
        createContext("/indices/nested/" + blobName);

        httpServer.start();
    }

    private static void createContext(String path) {
        httpServer.createContext(path, (exchange) -> {
            exchange.sendResponseHeaders(200, message.length);
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(message);
            }
        });
    }

    @AfterClass
    public static void stopHttp() throws IOException {
        httpServer.stop(0);
        httpServer = null;
    }

    @Before
    public void storeSetup() throws MalformedURLException {
        Settings settings = Settings.EMPTY;
        String spec = "http://localhost:6001/";
        urlBlobStore = new URLBlobStore(settings, new URL(spec));
    }

    public void testURLBlobStoreCanReadBlob() throws IOException {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        try (InputStream stream = container.readBlob(blobName)) {
            byte[] bytes = new byte[message.length];
            int read = stream.read(bytes);
            assertEquals(message.length, read);
            assertArrayEquals(message, bytes);
        }
    }

    public void testURLBlobStoreCanReadNestedBlob() throws IOException {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        try (InputStream stream = container.readBlob("nested/" + blobName)) {
            byte[] bytes = new byte[message.length];
            int read = stream.read(bytes);
            assertEquals(message.length, read);
            assertArrayEquals(message, bytes);
        }
    }

    public void testURLBlobStoreCanReadBlobWithSlashlessBase() throws IOException {
        URLBlobStore slashlessUrlBlobStore = new URLBlobStore(Settings.EMPTY, new URL("http://localhost:6001/indices"));
        BlobContainer container = slashlessUrlBlobStore.blobContainer(BlobPath.cleanPath());
        try (InputStream stream = container.readBlob(blobName)) {
            byte[] bytes = new byte[message.length];
            int read = stream.read(bytes);
            assertEquals(message.length, read);
            assertArrayEquals(message, bytes);
        }
    }

    public void testURLBlobStoreCanBuildNestedPathFromSlashlessBase() throws IOException {
        URLBlobStore slashlessUrlBlobStore = new URLBlobStore(Settings.EMPTY, new URL("http://localhost:6001/indices"));
        BlobContainer container = slashlessUrlBlobStore.blobContainer(BlobPath.cleanPath().add("nested"));
        try (InputStream stream = container.readBlob(blobName)) {
            byte[] bytes = new byte[message.length];
            int read = stream.read(bytes);
            assertEquals(message.length, read);
            assertArrayEquals(message, bytes);
        }
    }

    public void testURLBlobStoreRejectsPathTraversal() {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        List<String> invalidBlobNames = List.of(
            "../" + blobName,
            "..%2F" + blobName,
            "%2e%2e%2F" + blobName,
            "%252e%252e%252F" + blobName,
            "%2F" + blobName,
            "%5C" + blobName,
            "%C0%AF" + blobName,
            "%E0%80%AF" + blobName,
            "%41" + blobName,
            blobName + "%",
            blobName + "%A",
            blobName + "%GG",
            "../" + blobName + "#ignored",
            "/" + blobName,
            "//localhost:6001/" + blobName,
            "http://localhost:6001/" + blobName,
            "..\\\\" + blobName
        );

        for (String invalidBlobName : invalidBlobNames) {
            IOException exception = expectThrows(IOException.class, () -> container.readBlob(invalidBlobName));
            assertEquals("invalid blob name [" + invalidBlobName + "]", exception.getMessage());
        }
    }

    public void testURLBlobStoreRejectsEscapingBlobPaths() throws IOException {
        URLBlobStore slashlessUrlBlobStore = new URLBlobStore(Settings.EMPTY, new URL("http://localhost:6001/indices"));
        List<String> invalidPathElements = List.of(
            "..",
            "../indices-sibling",
            "%2e%2e",
            "%252e%252e",
            "/" + blobName,
            "//localhost:6001/" + blobName,
            "http://localhost:6001/" + blobName
        );

        for (String invalidPathElement : invalidPathElements) {
            BlobPath blobPath = BlobPath.cleanPath().add("nested").add(invalidPathElement);
            BlobStoreException exception = expectThrows(BlobStoreException.class, () -> slashlessUrlBlobStore.blobContainer(blobPath));
            assertEquals("malformed URL " + blobPath, exception.getMessage());
            assertThat(exception.getCause().getMessage(), containsString("invalid URL path"));
        }
    }

    public void testNoBlobFound() throws IOException {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        String incorrectBlobName = "incorrect_" + blobName;
        try (InputStream ignored = container.readBlob(incorrectBlobName)) {
            fail("Should have thrown NoSuchFileException exception");
            ignored.read();
        } catch (NoSuchFileException e) {
            assertEquals(String.format("[%s] blob not found", incorrectBlobName), e.getMessage());
        }
    }
}
