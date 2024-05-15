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

package org.opensearch.ingest.attachment;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.tika.metadata.Metadata;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Parse sample tika documents and assert the contents has not changed according to previously recorded checksums.
 * Uncaught changes to tika parsing could potentially pose bwc issues.
 * Note: In some cases tika will access a user's locale to inform the parsing of a file.
 * The checksums of these files are left empty, and we only validate that parsed content is not null.
 */
@SuppressFileSystems("ExtrasFS") // don't try to parse extraN
public class TikaDocTests extends OpenSearchTestCase {

    /** some test files from the apache tika unit test suite with accompanying sha1 checksums */
    static final String TIKA_FILES = "/org/opensearch/ingest/attachment/test/tika-files/";
    static final String TIKA_CHECKSUMS = "/org/opensearch/ingest/attachment/test/.checksums";

    public void testParseSamples() throws Exception {
        String checksumJson = Files.readString(PathUtils.get(getClass().getResource(TIKA_CHECKSUMS).toURI()));
        Map<String, Object> checksums = XContentHelper.convertToMap(JsonXContent.jsonXContent, checksumJson, false);
        DirectoryStream<Path> stream = Files.newDirectoryStream(unzipToTemp(TIKA_FILES));

        for (Path doc : stream) {
            String parsedContent = tryParse(doc);
            assertNotNull(parsedContent);
            assertFalse(parsedContent.isEmpty());

            String check = checksums.get(doc.getFileName().toString()).toString();
            if (!check.isEmpty()) {
                assertEquals(check, DigestUtils.sha1Hex(parsedContent));
            }
        }

        stream.close();
    }

    private Path unzipToTemp(String zipDir) throws Exception {
        Path tmp = createTempDir();
        DirectoryStream<Path> stream = Files.newDirectoryStream(PathUtils.get(getClass().getResource(zipDir).toURI()));

        for (Path doc : stream) {
            String filename = doc.getFileName().toString();
            TestUtil.unzip(getClass().getResourceAsStream(zipDir + filename), tmp);
        }

        stream.close();
        return tmp;
    }

    private String tryParse(Path doc) throws Exception {
        byte bytes[] = Files.readAllBytes(doc);
        return TikaImpl.parse(bytes, new Metadata(), -1);
    }
}
