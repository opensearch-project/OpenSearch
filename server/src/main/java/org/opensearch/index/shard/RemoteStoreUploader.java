/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.core.action.ActionListener;

import java.util.Collection;
import java.util.Map;

/**
 * Interface to handle the functionality for upload data in the remote store
 */
public interface RemoteStoreUploader {

    void syncAndUploadNewSegments(Collection<String> localSegments, Map<String, Long> localSegmentsSizeMap, ActionListener<Void> listener);
}
