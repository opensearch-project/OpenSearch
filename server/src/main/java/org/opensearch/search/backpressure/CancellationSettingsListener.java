/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

/**
 * Listener for callbacks related to cancellation settings
 */
public interface CancellationSettingsListener {

    void onRatioChanged(double ratio);

    void onRateChanged(double rate);

    void onBurstChanged(double burst);
}
