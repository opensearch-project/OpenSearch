/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery.ec2;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;

import java.nio.file.Path;

import software.amazon.awssdk.profiles.ProfileFileSystemSetting;

/**
 * The trait that adds the config path and AWS profile setup to the test cases.
 */
interface ConfigPathSupport {
    default Path configPath() {
        return PathUtils.get("config");
    }

    @SuppressForbidden(reason = "set predictable aws defaults")
    default public void setUpAwsProfile() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.region", "us-west-2"));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.accessKeyId", "aws-access-key-id"));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.secretAccessKey", "aws-secret-access-key"));
        SocketAccess.doPrivilegedVoid(AwsEc2ServiceImpl::setDefaultAwsProfilePath);
    }

    @SuppressForbidden(reason = "reset aws settings")
    default public void resetAwsProfile() throws Exception {
        SocketAccess.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        SocketAccess.doPrivileged(() -> System.clearProperty("aws.region"));
        SocketAccess.doPrivileged(() -> System.clearProperty("aws.accessKeyId"));
        SocketAccess.doPrivileged(() -> System.clearProperty("aws.secretAccessKey"));
        SocketAccess.doPrivileged(() -> System.clearProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property()));
        SocketAccess.doPrivileged(() -> System.clearProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property()));
    }
}
