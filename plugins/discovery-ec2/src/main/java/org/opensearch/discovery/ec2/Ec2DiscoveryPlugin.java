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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.node.Node;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.transport.TransportService;
import software.amazon.awssdk.core.SdkSystemSetting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        final Settings.Builder builder = Settings.builder();

        // Adds a node attribute for the ec2 availability zone
        Optional<String> ec2MetadataServiceEndpoint = SdkSystemSetting.AWS_EC2_METADATA_SERVICE_ENDPOINT.getStringValue();
        if (ec2MetadataServiceEndpoint.isPresent()) {
            builder.put(
                getAvailabilityZoneNodeAttributes(
                    settings,
                    ec2MetadataServiceEndpoint.get() + "/latest/meta-data/placement/availability-zone"
                )
            );
        }
        return builder.build();
    }

    // pkg private for testing
    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    static Settings getAvailabilityZoneNodeAttributes(Settings settings, String azMetadataUrl) {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings) == false) {
            return Settings.EMPTY;
        }
        final Settings.Builder attrs = Settings.builder();

        final URL url;
        final URLConnection urlConnection;
        try {
            url = new URL(azMetadataUrl);
            // Obtain the current EC2 instance availability zone from IMDS.
            // Same as curl http://169.254.169.254/latest/meta-data/placement/availability-zone/.
            // TODO: use EC2MetadataUtils::getAvailabilityZone that was added in AWS SDK v2 instead of rolling our own
            logger.debug("obtaining ec2 [placement/availability-zone] from ec2 meta-data url {}", url);
            urlConnection = SocketAccess.doPrivilegedIOException(url::openConnection);
            urlConnection.setConnectTimeout(2000);
        } catch (final IOException e) {
            // should not happen, we know the url is not malformed, and openConnection does not actually hit network
            throw new UncheckedIOException(e);
        }

        try (
            InputStream in = SocketAccess.doPrivilegedIOException(urlConnection::getInputStream);
            BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {

            final String metadataResult = urlReader.readLine();
            if ((metadataResult == null) || (metadataResult.length() == 0)) {
                throw new IllegalStateException("no ec2 metadata returned from " + url);
            } else {
                attrs.put(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone", metadataResult);
            }
        } catch (final IOException e) {
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
