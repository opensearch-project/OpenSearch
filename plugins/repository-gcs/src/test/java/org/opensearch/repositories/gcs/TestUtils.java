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

package org.opensearch.repositories.gcs;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

final class TestUtils {

    private TestUtils() {}

    /**
     * Creates a random Service Account file for testing purpose
     */
    static byte[] createServiceAccount(final Random random) {
        try {
            final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            final String privateKey = Base64.getEncoder().encodeToString(keyPairGenerator.generateKeyPair().getPrivate().getEncoded());

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (XContentBuilder builder = new XContentBuilder(MediaTypeRegistry.JSON.xContent(), out)) {
                builder.startObject();
                {
                    builder.field("type", "service_account");
                    builder.field("project_id", "test");
                    builder.field("private_key_id", UUID.randomUUID().toString());
                    builder.field("private_key", "-----BEGIN PRIVATE KEY-----\n" + privateKey + "\n-----END PRIVATE KEY-----\n");
                    builder.field("client_email", "opensearch@appspot.gserviceaccount.com");
                    builder.field("client_id", String.valueOf(Math.abs(random.nextLong())));
                }
                builder.endObject();
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new AssertionError("Unable to create service account file", e);
        }
    }
}
