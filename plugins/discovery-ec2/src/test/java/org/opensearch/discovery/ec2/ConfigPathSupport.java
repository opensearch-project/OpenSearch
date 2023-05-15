/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery.ec2;

import org.opensearch.common.io.PathUtils;

import java.nio.file.Path;

/**
 * The trait that adds the config path and AWS profile setup to the test cases.
 */
interface ConfigPathSupport {
    default Path configPath() {
        return PathUtils.get("config");
    }

    default public void setUpAwsProfile() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.region", "us-east-1"));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.accessKeyId", "aws-access-key-id"));
        SocketAccess.doPrivileged(() -> System.setProperty("aws.secretAccessKey", "aws-secret-access-key"));
        SocketAccess.doPrivilegedVoid(AwsEc2ServiceImpl::setDefaultAwsProfilePath);
    }
}
