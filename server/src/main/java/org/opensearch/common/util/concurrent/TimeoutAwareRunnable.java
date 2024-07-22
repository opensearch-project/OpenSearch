/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

/**
 * Runnable that is aware of a timeout and can execute another {@link Runnable} when a timeout is reached
 */
public interface TimeoutAwareRunnable extends Runnable {

    void onTimeout();
}
