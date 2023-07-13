/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.io;

public class CheckedContainer {

    private String checksum;
    private long contentLength;

    public CheckedContainer(long contentLength) {
        this.contentLength = contentLength;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getContentLength() {
        return contentLength;
    }
}
