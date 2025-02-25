/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.Optional;

/**
 * A security setting provider for auxiliary transports.
 * As auxiliary transports are pluggable, SSLContextBuilder is provided as a generic way for transports
 * to construct a ssl context for their particular transport implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureAuxTransportSettingsProvider {
    /**
     * Builds an {@link SSLEngine} instance.
     * @return if supported, builds the {@link SSLEngine} instance.
     * @throws SSLException throws SSLException if the {@link SSLEngine} instance cannot be built.
     */
    Optional<SSLEngine> buildSecureAuxServerEngine() throws SSLException;
}
