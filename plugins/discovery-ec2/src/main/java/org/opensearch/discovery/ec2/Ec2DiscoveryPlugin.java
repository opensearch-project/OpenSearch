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

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.imds.Ec2MetadataClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.node.Node;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Ec2DiscoveryPlugin extends Plugin implements DiscoveryPlugin, ReloadablePlugin {

    private static Logger logger = LogManager.getLogger(Ec2DiscoveryPlugin.class);
    public static final String EC2 = "ec2";

    static {
        SpecialPermission.check();
    }

    private final Settings settings;
    // protected for testing
    protected final AwsEc2Service ec2Service;

    public Ec2DiscoveryPlugin(Settings settings) {
        this(settings, new AwsEc2ServiceImpl());
    }

    protected Ec2DiscoveryPlugin(Settings settings, AwsEc2ServiceImpl ec2Service) {
        this.settings = settings;
        this.ec2Service = ec2Service;
        // eagerly load client settings when secure settings are accessible
        reload(settings);
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
        logger.debug("Register _ec2_, _ec2:xxx_ network names");
        return new Ec2NameResolver();
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(EC2, () -> new AwsEc2SeedHostsProvider(settings, transportService, ec2Service));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // Register EC2 discovery settings: discovery.ec2
            Ec2ClientSettings.ACCESS_KEY_SETTING,
            Ec2ClientSettings.SECRET_KEY_SETTING,
            Ec2ClientSettings.SESSION_TOKEN_SETTING,
            Ec2ClientSettings.ENDPOINT_SETTING,
            Ec2ClientSettings.REGION_SETTING,
            Ec2ClientSettings.PROTOCOL_SETTING,
            Ec2ClientSettings.PROXY_HOST_SETTING,
            Ec2ClientSettings.PROXY_PORT_SETTING,
            Ec2ClientSettings.PROXY_USERNAME_SETTING,
            Ec2ClientSettings.PROXY_PASSWORD_SETTING,
            Ec2ClientSettings.READ_TIMEOUT_SETTING,
            AwsEc2Service.HOST_TYPE_SETTING,
            AwsEc2Service.ANY_GROUP_SETTING,
            AwsEc2Service.GROUPS_SETTING,
            AwsEc2Service.AVAILABILITY_ZONES_SETTING,
            AwsEc2Service.NODE_CACHE_TIME_SETTING,
            AwsEc2Service.TAG_SETTING,
            // Register cloud node settings: cloud.node
            AwsEc2Service.AUTO_ATTRIBUTE_SETTING
        );
    }

    @Override
    public Settings additionalSettings() {
        return getAvailabilityZoneNodeAttributes(settings);
    }

    // pkg private for testing
    static Settings getAvailabilityZoneNodeAttributes(Settings settings) {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings) == false) {
            return Settings.EMPTY;
        }

        try (
            Ec2MetadataClient client = AccessController.doPrivilegedChecked(
                () -> Ec2MetadataClient.builder()
                    .httpClient(ApacheHttpClient.builder().connectionTimeout(Duration.ofSeconds(2)).socketTimeout(Duration.ofSeconds(2)))
                    .build()
            )
        ) {
            return getAvailabilityZoneNodeAttributes(settings, client);
        } catch (final Exception e) {
            // this is lenient so the plugin does not fail when installed outside of ec2
            logger.error("failed to get metadata for [placement/availability-zone]", e);
            return Settings.EMPTY;
        }
    }

    // pkg private for testing
    static Settings getAvailabilityZoneNodeAttributes(Settings settings, Ec2MetadataClient client) {
        final Settings.Builder attrs = Settings.builder();

        try {
            logger.debug("obtaining ec2 [placement/availability-zone] from IMDS");
            final String az = AccessController.doPrivilegedChecked(
                () -> client.get("/latest/meta-data/placement/availability-zone").asString()
            );

            if (az == null || az.isEmpty()) {
                throw new IllegalStateException("no ec2 metadata returned from IMDS");
            } else {
                attrs.put(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone", az);
            }
        } catch (final Exception e) {
            // this is lenient so the plugin does not fail when installed outside of ec2
            logger.error("failed to get metadata for [placement/availability-zone]", e);
        }

        return attrs.build();
    }

    @Override
    public void close() throws IOException {
        ec2Service.close();
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Ec2ClientSettings clientSettings = Ec2ClientSettings.getClientSettings(settings);
        ec2Service.refreshAndClearCache(clientSettings);
    }
}
