/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.core.common.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Objects;

public class ProxySettings {

    public static final ProxySettings NO_PROXY_SETTINGS = new ProxySettings(Proxy.Type.DIRECT, null, -1, null, null);

    private final Proxy.Type type;

    private final InetAddress host;

    private final String username;

    private final String password;

    private final int port;

    public ProxySettings(final Proxy.Type type, final InetAddress host, final int port, final String username, final String password) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public Proxy.Type getType() {
        return this.type;
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public boolean isAuthenticated() {
        return Strings.isNullOrEmpty(username) == false && Strings.isNullOrEmpty(password) == false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProxySettings that = (ProxySettings) o;
        return port == that.port
            && type == that.type
            && Objects.equals(host, that.host)
            && Objects.equals(username, that.username)
            && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, host, username, password, port);
    }
}
