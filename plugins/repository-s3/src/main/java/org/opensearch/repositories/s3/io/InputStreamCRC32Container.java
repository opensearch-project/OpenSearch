/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.io;

import org.opensearch.common.io.InputStreamContainer;

import java.io.InputStream;

public class InputStreamCRC32Container extends InputStreamContainer {

    private String checksum;

    public InputStreamCRC32Container(InputStream inputStream, long contentLength) {
        super(inputStream, contentLength);
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getChecksum() {
        return checksum;
    }
}
