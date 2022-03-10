/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.amazonaws.Protocol;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.SettingsException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class ProxySettings {
    public static final ProxySettings NO_PROXY_SETTINGS = new ProxySettings(ProxyType.DIRECT, null, -1, null, null);

    public static enum ProxyType {
        HTTP(Protocol.HTTP.name()),
        HTTPS(Protocol.HTTPS.name()),
        SOCKS("SOCKS"),
        DIRECT("DIRECT");

        private final String name;

        private ProxyType(String name) {
            this.name = name;
        }

        public Protocol toProtocol() {
            if (this == DIRECT) {
                // We check it in settings,
                // the probability that it could be thrown is small, but how knows
                throw new SettingsException("Couldn't convert to S3 protocol");
            } else if (this == SOCKS) {
                throw new SettingsException("Couldn't convert to S3 protocol. SOCKS is not supported");
            }
            return Protocol.valueOf(name());
        }

    }

    private final ProxyType type;

    private final String host;

    private final String username;

    private final String password;

    private final int port;

    public String getHost() {
        return host;
    }

    public ProxySettings(final ProxyType type, final String host, final int port, final String username, final String password) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public ProxyType getType() {
        return this.type;
    }

    public String getHostName() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public InetSocketAddress getAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            // this error won't be thrown since validation of the host name is in the S3ClientSettings
            throw new RuntimeException(e);
        }
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

    public ProxySettings recreateWithNewHostAndPort(final String host, final int port) {
        return new ProxySettings(type, host, port, username, password);
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
