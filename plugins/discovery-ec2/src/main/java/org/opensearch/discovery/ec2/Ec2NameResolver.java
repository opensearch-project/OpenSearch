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

package org.opensearch.discovery.ec2;

import software.amazon.awssdk.imds.Ec2MetadataClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService.CustomNameResolver;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Resolves certain ec2 related 'meta' hostnames into an actual hostname
 * obtained from ec2 meta-data.
 * <p>
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 * <li>_ec2_ - maps to privateIpv4</li>
 * <li>_ec2:privateIp_ - maps to privateIpv4</li>
 * <li>_ec2:privateIpv4_</li>
 * <li>_ec2:privateDns_</li>
 * <li>_ec2:publicIp_ - maps to publicIpv4</li>
 * <li>_ec2:publicIpv4_</li>
 * <li>_ec2:publicDns_</li>
 * </ul>
 *
 * @author Paul_Loy (keteracel)
 */
class Ec2NameResolver implements CustomNameResolver {

    private static final Logger logger = LogManager.getLogger(Ec2NameResolver.class);

    private final AwsEc2Service ec2Service;

    Ec2NameResolver(AwsEc2Service ec2Service) {
        this.ec2Service = ec2Service;
    }

    /**
     * enum that can be added to over time with more meta-data types (such as ipv6 when this is available)
     *
     * @author Paul_Loy
     */
    private enum Ec2HostnameType {

        PRIVATE_IPv4("ec2:privateIpv4", "local-ipv4"),
        PRIVATE_DNS("ec2:privateDns", "local-hostname"),
        PUBLIC_IPv4("ec2:publicIpv4", "public-ipv4"),
        PUBLIC_DNS("ec2:publicDns", "public-hostname"),

        // some less verbose defaults
        PUBLIC_IP("ec2:publicIp", PUBLIC_IPv4.ec2Name),
        PRIVATE_IP("ec2:privateIp", PRIVATE_IPv4.ec2Name),
        EC2("ec2", PRIVATE_IPv4.ec2Name);

        final String configName;
        final String ec2Name;

        Ec2HostnameType(String configName, String ec2Name) {
            this.configName = configName;
            this.ec2Name = ec2Name;
        }
    }

    /**
     * @param type the ec2 hostname type to discover.
     * @return the appropriate host resolved from ec2 meta-data, or null if it cannot be obtained.
     * @see CustomNameResolver#resolveIfPossible(String)
     */
    public InetAddress[] resolve(Ec2HostnameType type) throws IOException {
        try (AmazonEc2MetadataClientReference clientReference = ec2Service.metadataClient()) {
            return resolve(type, clientReference.get());
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to fetch ec2 metadata for [" + type.configName + "]", e);
        }
    }

    // pkg private for testing
    InetAddress[] resolve(Ec2HostnameType type, Ec2MetadataClient client) throws IOException {
        final String path = "/latest/meta-data/" + type.ec2Name;
        logger.debug("obtaining ec2 hostname from IMDS path {}", path);
        final String result;
        try {
            result = AccessController.doPrivilegedChecked(() -> client.get(path).asString());
        } catch (Exception e) {
            throw new IOException("IOException caught when fetching InetAddress for [" + type.configName + "]", e);
        }
        if (result == null || result.isEmpty()) {
            throw new IOException("no ec2 metadata returned from IMDS for [" + type.configName + "]");
        }
        logger.debug("obtained ec2 hostname from IMDS for {}: {}", type.configName, result);
        // only one address: because we explicitly ask for only one via the Ec2HostnameType
        return new InetAddress[] { InetAddress.getByName(result) };
    }

    @Override
    public InetAddress[] resolveDefault() {
        return null; // using this, one has to explicitly specify _ec2_ in network setting
        // return resolve(Ec2HostnameType.DEFAULT, false);
    }

    @Override
    public InetAddress[] resolveIfPossible(String value) throws IOException {
        for (Ec2HostnameType type : Ec2HostnameType.values()) {
            if (type.configName.equals(value)) {
                return resolve(type);
            }
        }
        return null;
    }

}
