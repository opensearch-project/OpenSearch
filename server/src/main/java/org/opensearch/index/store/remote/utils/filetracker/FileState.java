/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.filetracker;

public enum FileState {
    /**
     * DISK State means that currently the file is present only locally and has not yet been uploaded to the Remote Store
     */
    DISK,
    /**
     * REMOTE State means that the file has been successfully uploaded to the Remote Store and is safe to be removed locally
     */
    REMOTE;
}
