/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import com.azure.core.http.ProxyOptions;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.core.common.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

public class ProxySettings {

    public static final ProxySettings NO_PROXY_SETTINGS = new ProxySettings(ProxyType.DIRECT, null, -1, null, null);

    private final ProxyType type;

    private final InetAddress host;

    private final String username;

    private final String password;

    private final int port;

    public static enum ProxyType {
        HTTP(ProxyOptions.Type.HTTP.name()),

        /**
         * Please use SOCKS4 instead
         */
        @Deprecated
        SOCKS(ProxyOptions.Type.SOCKS4.name()),

        SOCKS4(ProxyOptions.Type.SOCKS4.name()),

        SOCKS5(ProxyOptions.Type.SOCKS5.name()),

        DIRECT("DIRECT");

        private final String name;

        private ProxyType(String name) {
            this.name = name;
        }

        public ProxyOptions.Type toProxyType() {
            if (this == DIRECT) {
                // We check it in settings,
                // the probability that it could be thrown is small, but how knows
                throw new SettingsException("Couldn't convert to Azure proxy type");
            }
            return ProxyOptions.Type.valueOf(name());
        }

    }

    public ProxySettings(final ProxyType type, final InetAddress host, final int port, final String username, final String password) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public ProxyType getType() {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
