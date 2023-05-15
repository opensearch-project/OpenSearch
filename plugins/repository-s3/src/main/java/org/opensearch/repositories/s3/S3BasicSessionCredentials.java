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

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Objects;

final class S3BasicSessionCredentials extends S3BasicCredentials {

    private final AwsSessionCredentials sessionCredentials;

    S3BasicSessionCredentials(String accessKey, String secretKey, String sessionToken) {
        super(accessKey, secretKey);
        sessionCredentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
    }

    public String getSessionToken() {
        return sessionCredentials.sessionToken();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final S3BasicSessionCredentials that = (S3BasicSessionCredentials) o;
        return getSessionToken().equals(that.getSessionToken())
            && accessKeyId().equals(that.accessKeyId())
            && secretAccessKey().equals(that.secretAccessKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSessionToken(), accessKeyId(), secretAccessKey());
    }
}
