/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.profiles.ProfileFileSystemSetting;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

public abstract class AbstractS3RepositoryTestCase extends OpenSearchTestCase {
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

    protected Path configPath() {
        return PathUtils.get("config");
    }

    private String previousOpenSearchPathConf;
    private String awsSharedCredentialsFile;
    private String awsConfigFile;

    @SuppressForbidden(reason = "set predictable aws defaults")
    private void setUpAwsProfile() throws Exception {
        previousOpenSearchPathConf = AccessController.doPrivileged(
            () -> System.setProperty("opensearch.path.conf", configPath().toString())
        );
        awsSharedCredentialsFile = System.getProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property());
        awsConfigFile = System.getProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property());
        AccessController.doPrivileged(S3Service::setDefaultAwsProfilePath);
    }

    @SuppressForbidden(reason = "reset aws settings")
    private void resetAwsProfile() throws Exception {
        resetPropertyValue("opensearch.path.conf", previousOpenSearchPathConf);
        resetPropertyValue(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(), awsSharedCredentialsFile);
        resetPropertyValue(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), awsConfigFile);
    }

    @SuppressForbidden(reason = "reset aws settings")
    private void resetPropertyValue(String key, String value) {
        if (value != null) {
            AccessController.doPrivileged(() -> System.setProperty(key, value));
        } else {
            AccessController.doPrivileged(() -> System.clearProperty(key));
        }
    }
}
