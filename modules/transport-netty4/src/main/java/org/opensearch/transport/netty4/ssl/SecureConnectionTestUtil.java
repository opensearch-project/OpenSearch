/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport.netty4.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Utility class to test if the server supports SSL connections.
 * SSL Check will be done by sending an OpenSearch Ping to see if server is replying to pings.
 * Following that a custom client hello message will be sent to the server, if the server
 * side has OpenSearchPortUnificationHandler it will reply with server hello message.
 *
 * @see <a href="https://github.com/opensearch-project/security/blob/d526c9f6c2a438c14db8b413148204510b9fe2e2/src/main/java/org/opensearch/security/ssl/util/SSLConnectionTestUtil.java">SSLConnectionTestUtil</a>
 */
class SecureConnectionTestUtil {
    private static final Logger logger = LogManager.getLogger(SecureConnectionTestUtil.class);

    /**
     * Return codes for SSLConnectionTestUtil.testConnection()
     */
    enum SSLConnectionTestResult {
        /**
         * OpenSearch Ping to the server failed.
         */
        OPENSEARCH_PING_FAILED,
        /**
         * Server does not support SSL.
         */
        SSL_NOT_AVAILABLE,
        /**
         * Server supports SSL.
         */
        SSL_AVAILABLE
    }

    public static final byte[] OPENSEARCH_PING_MSG = new byte[] {
        (byte) 'E',
        (byte) 'S',
        (byte) 0xFF,
        (byte) 0xFF,
        (byte) 0xFF,
        (byte) 0xFF };
    public static final String DUAL_MODE_CLIENT_HELLO_MSG = "DUALCM";
    public static final String DUAL_MODE_SERVER_HELLO_MSG = "DUALSM";
    private static final int SOCKET_TIMEOUT_MILLIS = 10 * 1000;
    private final String host;
    private final int port;
    private Socket overriddenSocket = null;
    private OutputStreamWriter testOutputStreamWriter = null;
    private InputStreamReader testInputStreamReader = null;

    public SecureConnectionTestUtil(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    protected SecureConnectionTestUtil(
        final String host,
        final int port,
        final Socket overriddenSocket,
        final OutputStreamWriter testOutputStreamWriter,
        final InputStreamReader testInputStreamReader
    ) {
        this.overriddenSocket = overriddenSocket;
        this.testOutputStreamWriter = testOutputStreamWriter;
        this.testInputStreamReader = testInputStreamReader;

        this.host = host;
        this.port = port;
    }

    /**
     * Test connection to server by performing the below steps:
     * - Send Client Hello to check if the server replies with Server Hello which indicates that Server understands SSL
     * - Send OpenSearch Ping to check if the server replies to the OpenSearch Ping message
     *
     * @return SSLConnectionTestResult i.e. OPENSEARCH_PING_FAILED or SSL_NOT_AVAILABLE or SSL_AVAILABLE
     */
    public SSLConnectionTestResult testConnection() {
        if (sendDualSSLClientHello()) {
            return SSLConnectionTestResult.SSL_AVAILABLE;
        }

        if (sendOpenSearchPing()) {
            return SSLConnectionTestResult.SSL_NOT_AVAILABLE;
        }

        return SSLConnectionTestResult.OPENSEARCH_PING_FAILED;
    }

    private boolean sendDualSSLClientHello() {
        boolean dualSslSupported = false;
        Socket socket = null;
        try {
            OutputStreamWriter outputStreamWriter;
            InputStreamReader inputStreamReader;
            if (overriddenSocket != null) {
                socket = overriddenSocket;
                outputStreamWriter = testOutputStreamWriter;
                inputStreamReader = testInputStreamReader;
            } else {
                socket = new Socket(InetAddress.getByName(host), port);
                outputStreamWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
                inputStreamReader = new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8);
            }

            socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
            outputStreamWriter.write(DUAL_MODE_CLIENT_HELLO_MSG);
            outputStreamWriter.flush();
            logger.debug("Sent DualSSL Client Hello msg to {}", host);

            StringBuilder sb = new StringBuilder();
            int currentChar;
            while ((currentChar = inputStreamReader.read()) != -1) {
                sb.append((char) currentChar);
            }

            if (sb.toString().equals(DUAL_MODE_SERVER_HELLO_MSG)) {
                logger.debug("Received DualSSL Server Hello msg from {}", host);
                dualSslSupported = true;
            }
        } catch (IOException e) {
            logger.debug("DualSSL client check failed for {}, exception {}", host, e.getMessage());
        } finally {
            logger.debug("Closing DualSSL check client socket for {}", host);
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(
                        "Exception occurred while closing DualSSL check client socket for {}. Exception: {}",
                        host,
                        e.getMessage()
                    );
                }
            }
        }
        logger.debug("dualSslClient check with server {}, server supports ssl = {}", host, dualSslSupported);
        return dualSslSupported;
    }

    private boolean sendOpenSearchPing() {
        boolean pingSucceeded = false;
        Socket socket = null;
        try {
            if (overriddenSocket != null) {
                socket = overriddenSocket;
            } else {
                socket = new Socket(InetAddress.getByName(host), port);
            }

            socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            logger.debug("Sending OpenSearch Ping to {}", host);
            outputStream.write(OPENSEARCH_PING_MSG);
            outputStream.flush();

            int currentByte;
            int byteBufIndex = 0;
            byte[] response = new byte[6];
            while ((byteBufIndex < 6) && ((currentByte = inputStream.read()) != -1)) {
                response[byteBufIndex] = (byte) currentByte;
                byteBufIndex++;
            }
            if (byteBufIndex == 6) {
                logger.debug("Received reply for OpenSearch Ping. from {}", host);
                pingSucceeded = true;
                for (int i = 0; i < 6; i++) {
                    if (response[i] != OPENSEARCH_PING_MSG[i]) {
                        // Unexpected byte in response
                        logger.error("Received unexpected byte in OpenSearch Ping reply from {}", host);
                        pingSucceeded = false;
                        break;
                    }
                }
            }
        } catch (IOException ex) {
            logger.error("OpenSearch Ping failed for {}, exception: {}", host, ex.getMessage());
        } finally {
            logger.debug("Closing OpenSearch Ping client socket for connection to {}", host);
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error("Exception occurred while closing socket for {}. Exception: {}", host, e.getMessage());
                }
            }
        }

        logger.debug("OpenSearch Ping check to server {} result = {}", host, pingSucceeded);
        return pingSucceeded;
    }
}
