/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;

import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.test.OpenSearchTestCase;

public class TransferManagerUtilsTests extends OpenSearchTestCase {

    public void testGetOptimalPartSizeGreaterValueInConfiguration() {
        long contentLength = ByteSizeUnit.MB.toBytes(100);
        long minimumPartSizeSetting = ByteSizeUnit.MB.toBytes(5);
        long configurationPartSizeSetting = ByteSizeUnit.MB.toBytes(10);

        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMinimumUploadPartSize(configurationPartSizeSetting);

        long optimalPartSize = TransferManagerUtils.getOptimalPartSize(
            contentLength,
            transferManagerConfiguration,
            minimumPartSizeSetting
        );

        assertEquals(configurationPartSizeSetting, optimalPartSize);
    }

    public void testGetOptimalPartSizeGreaterValueInPluginSetting() {
        long contentLength = ByteSizeUnit.MB.toBytes(100);
        long minimumPartSizeSetting = ByteSizeUnit.MB.toBytes(10);
        long configurationPartSizeSetting = ByteSizeUnit.MB.toBytes(5);

        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMinimumUploadPartSize(configurationPartSizeSetting);

        long optimalPartSize = TransferManagerUtils.getOptimalPartSize(
            contentLength,
            transferManagerConfiguration,
            minimumPartSizeSetting
        );

        assertEquals(minimumPartSizeSetting, optimalPartSize);
    }

    public void testGetOptimalPartSizeContentLengthLessThanUploadThreshold() {
        long contentLength = ByteSizeUnit.MB.toBytes(10);
        long minimumPartSizeSetting = ByteSizeUnit.MB.toBytes(5);
        long configurationPartSizeSetting = ByteSizeUnit.MB.toBytes(5);

        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMinimumUploadPartSize(configurationPartSizeSetting);
        transferManagerConfiguration.setMultipartUploadThreshold(ByteSizeUnit.MB.toBytes(16));

        long optimalPartSize = TransferManagerUtils.getOptimalPartSize(
            contentLength,
            transferManagerConfiguration,
            minimumPartSizeSetting
        );

        assertEquals(contentLength, optimalPartSize);
    }
}
