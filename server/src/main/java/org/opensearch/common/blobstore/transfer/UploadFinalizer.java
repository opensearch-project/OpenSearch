/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import java.io.IOException;

/**
 * UploadFinalizer is an interface with support for a method that will be called once upload is complete
 */
public interface UploadFinalizer {
    void accept(boolean uploadSuccess) throws IOException;
}
