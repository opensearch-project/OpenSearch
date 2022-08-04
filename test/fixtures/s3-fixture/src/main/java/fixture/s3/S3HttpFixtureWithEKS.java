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

package fixture.s3;

import com.sun.net.httpserver.HttpHandler;
import org.opensearch.rest.RestStatus;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class S3HttpFixtureWithEKS extends S3HttpFixture {

    private S3HttpFixtureWithEKS(final String[] args) throws Exception {
        super(args);
    }

    @Override
    protected HttpHandler createHandler(final String[] args) {
        final String accessKey = Objects.requireNonNull(args[4]);
        final String eksRoleArn = Objects.requireNonNull(args[5]);
        final HttpHandler delegate = super.createHandler(args);

        return exchange -> {
            // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
            if ("POST".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getPath().startsWith("/eks_credentials_endpoint")) {
                final byte[] response = buildCredentialResponse(eksRoleArn, accessKey).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;
            }

            delegate.handle(exchange);
        };
    }

    protected String buildCredentialResponse(final String roleArn, final String accessKey) {
        // See please: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
        return "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n"
                + "  <AssumeRoleWithWebIdentityResult>\n"
                + "    <SubjectFromWebIdentityToken>amzn1.account.AF6RHO7KZU5XRVQJGXK6HB56KR2A</SubjectFromWebIdentityToken>\n"
                + "    <Audience>client.5498841531868486423.1548@apps.example.com</Audience>\n"
                + "    <AssumedRoleUser>\n"
                + "      <Arn>" + roleArn + "</Arn>\n"
                + "      <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:s3</AssumedRoleId>\n"
                + "    </AssumedRoleUser>\n"
                + "    <Credentials>\n"
                + "      <SessionToken>AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE</SessionToken>\n"
                + "      <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>\n"
                + "      <Expiration>" + LocalDateTime.now().plusMonths(1).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "</Expiration>\n"
                + "      <AccessKeyId>" + accessKey + "</AccessKeyId>\n"
                + "    </Credentials>\n"
                + "    <SourceIdentity>SourceIdentityValue</SourceIdentity>\n"
                + "    <Provider>www.amazon.com</Provider>\n"
                + "  </AssumeRoleWithWebIdentityResult>\n"
                + "  <ResponseMetadata>\n"
                + "    <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>\n"
                + "  </ResponseMetadata>\n"
                + "</AssumeRoleWithWebIdentityResponse>";
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 6) {
            throw new IllegalArgumentException("S3HttpFixtureWithEKS expects 6 arguments " +
                "[address, port, bucket, base path, role arn, role session name]");
        }
        final S3HttpFixtureWithEKS fixture = new S3HttpFixtureWithEKS(args);
        fixture.start();
    }
}
