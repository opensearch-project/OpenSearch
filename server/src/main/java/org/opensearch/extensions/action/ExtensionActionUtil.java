/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * ExtensionActionUtil - a class for creating and processing remote requests using byte arrays.
 */
public class ExtensionActionUtil {
    private static final Logger logger = LogManager.getLogger(ExtensionActionUtil.class);

    /**
     * The Unicode UNIT SEPARATOR used to separate the Request class name and parameter bytes
     */
    public static final byte UNIT_SEPARATOR = (byte) '\u001F';

    /**
     * @param request an instance of a request extending {@link ActionRequest}, containing information about the
     * request being sent to the remote server. It is used to create a byte array containing the request data,
     * which will be sent to the remote server.
     * @return An Extension ActionRequest object that represents the deserialized data.
     * If an error occurred during the deserialization process, the method will return {@code null}.
     */
    public static byte[] createProxyRequestBytes(ActionRequest request) {
        byte[] requestClassBytes = request.getClass().getName().getBytes(StandardCharsets.UTF_8);
        byte[] requestBytes;

        try {
            requestBytes = convertParamsToBytes(request);
            return ByteBuffer.allocate(requestClassBytes.length + 1 + requestBytes.length)
                .put(requestClassBytes)
                .put(UNIT_SEPARATOR)
                .put(requestBytes)
                .array();
        } catch (Exception e) {
            logger.error("Error occurred while creating proxyRequestBytes");
        }
        return null;
    }

    /**
     * @param bytes the byte array containing the serialized data for the ExtensionActionRequest object.
     * @return An Extension ActionRequest object that represents the deserialized data. If an error occurred during the deserialization process, the method will return {@code null}.
     */
    public static ExtensionActionRequest createExtensionActionRequestFromBytes(byte[] bytes) {
        int delimPos = delimPos(bytes);
        try {
            StreamInput requestByteStream = StreamInput.wrap(Arrays.copyOfRange(bytes, delimPos + 1, bytes.length));
            Class<?> clazz = ExtensionActionRequest.class;
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            return (ExtensionActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.error("An error occurred while creating ExtensionActionRequest: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * @param  requestBytes is a byte array containing the request data, used by the "createActionRequest"
     * method to create an "ActionRequest" object, which represents the request model to be processed on the server.
     * @return an "Action Request" object representing the request model for processing on the server,
     * or {@code null} if the request data is invalid or null.
     */
    public static ActionRequest createActionRequest(byte[] requestBytes) {
        int delimPos = delimPos(requestBytes);
        String requestClassName = new String(Arrays.copyOfRange(requestBytes, 0, delimPos + 1), StandardCharsets.UTF_8).stripTrailing();
        try {
            Class<?> clazz = Class.forName(requestClassName);
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(Arrays.copyOfRange(requestBytes, delimPos + 1, requestBytes.length));
            return (ActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.error("An error occurred while creating ActionRequest: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Converts the given object of type T, which implements the {@link Writeable} interface, to a byte array.
     * @param <T> the type of the object to be converted to bytes, which must implement the {@link Writeable} interface.
     * @param actionParams the object of type T to be converted to bytes.
     * @return a byte array containing the serialized bytes of the given object, or {@code null} if the input is invalid or null.
     * @throws IOException if there was an error while writing to the output stream.
     */
    public static <T extends Writeable> byte[] convertParamsToBytes(T actionParams) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionParams.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    /**
     * Searches for the position of the unit separator byte in the given byte array.
     *
     * @param bytes the byte array to search for the unit separator byte.
     * @return the index of the unit separator byte in the byte array, or -1 if it was not found.
     */
    public static int delimPos(byte[] bytes) {
        for (int offset = 0; offset < bytes.length; ++offset) {
            if (bytes[offset] == ExtensionActionUtil.UNIT_SEPARATOR) {
                return offset;
            }
        }
        return -1;
    }
}
