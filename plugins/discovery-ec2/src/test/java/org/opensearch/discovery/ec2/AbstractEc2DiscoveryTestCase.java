/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery.ec2;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import java.nio.file.Path;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;

public abstract class AbstractEc2DiscoveryTestCase extends OpenSearchTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        setUpAwsProfile();
    }

    @Override
    public void tearDown() throws Exception {
        resetAwsProfile();
        super.tearDown();
    }

    private Path configPath() {
        return PathUtils.get("config");
    }

    private String previousOpenSearchPathConf;
    private String awsRegion;
    private String awsAccessKeyId;
    private String awsSecretAccessKey;
    private String awsSharedCredentialsFile;
    private String awsConfigFile;

    @SuppressForbidden(reason = "set predictable aws defaults")
    private void setUpAwsProfile() throws Exception {
        previousOpenSearchPathConf = SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        awsRegion = SocketAccess.doPrivileged(() -> System.setProperty("aws.region", "us-west-2"));
        awsAccessKeyId = SocketAccess.doPrivileged(() -> System.setProperty("aws.accessKeyId", "aws-access-key-id"));
        awsSecretAccessKey = SocketAccess.doPrivileged(() -> System.setProperty("aws.secretAccessKey", "aws-secret-access-key"));
        awsSharedCredentialsFile = System.getProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property());
        awsConfigFile = System.getProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property());
        SocketAccess.doPrivilegedVoid(AwsEc2ServiceImpl::setDefaultAwsProfilePath);
    }

    @SuppressForbidden(reason = "reset aws settings")
    private void resetAwsProfile() throws Exception {
        resetPropertyValue("opensearch.path.conf", previousOpenSearchPathConf);
        resetPropertyValue("aws.region", awsRegion);
        resetPropertyValue("aws.accessKeyId", awsAccessKeyId);
        resetPropertyValue("aws.secretAccessKey", awsSecretAccessKey);
        resetPropertyValue(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(), awsSharedCredentialsFile);
        resetPropertyValue(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), awsConfigFile);
    }

    @SuppressForbidden(reason = "reset aws settings")
    private void resetPropertyValue(String key, String value) {
        if (value != null) {
            SocketAccess.doPrivileged(() -> System.setProperty(key, value));
        } else {
            SocketAccess.doPrivileged(() -> System.clearProperty(key));
        }
    }
}
