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
     * @param request r is an object of the "Remote Extension Action Request" class, containing information about the
     * request being sent to the remote server. It is used to create a byte array containing the request data,
     * which will be sent to the remote server.
     * @return a byte array containing all the necessary information about the request to be sent to the remote server.
     * This byte array is constructed using the class name of the request, a unit separator, and the request data itself.
     */
    public static byte[] createProxyRequestBytes(ExtensionActionRequest request) {
        String requestClass = request.getClass().getName();
        byte[] requestClassBytes = requestClass.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(requestClassBytes.length + 1 + request.getRequestBytes().length)
            .put(requestClassBytes)
            .put(UNIT_SEPARATOR)
            .put(request.getRequestBytes())
            .array();
    }

    /**
     * @param bytes the byte array containing the serialized data for the ExtensionActionRequest object.
     * @return an ExtensionActionRequest object representing the deserialized data,
     * or {@code null} if an error occurred during the deserialization process.
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
    public static ActionRequest createActionRequest(ExtensionActionRequest requestBytes) {
        int delimPos = delimPos(requestBytes.getRequestBytes());
        String requestClassName = new String(Arrays.copyOfRange(requestBytes.getRequestBytes(), 0, delimPos + 1), StandardCharsets.UTF_8)
            .stripTrailing();
        ActionRequest actionRequest = null;
        try {
            Class<?> clazz = Class.forName(requestClassName);
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(
                Arrays.copyOfRange(requestBytes.getRequestBytes(), delimPos + 1, requestBytes.getRequestBytes().length)
            );
            actionRequest = (ActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.error("An error occurred while creating ActionRequest: " + e.getMessage(), e);
        }
        return actionRequest;
    }

    /**
     * @param actionRequest  is an instance of the Action Request class that is used to create an ExtensionActionRequest object.
     * @return an Extension ActionRequest object, or {@code null} if an error occurred during the creation process.
     */
    public static ExtensionActionRequest createExtensionActionRequest(ActionRequest actionRequest) {
        byte[] requestBytes;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            actionRequest.writeTo(out);
            requestBytes = BytesReference.toBytes(out.bytes());
            Class<?> clazz = ExtensionActionRequest.class;
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(
                Arrays.copyOfRange(requestBytes, delimPos(requestBytes) + 1, requestBytes.length)
            );
            return (ExtensionActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.error("An error occurred while creating ExtensionActionRequest: " + e.getMessage(), e);
        }
        return null;
    }

    public static int delimPos(byte[] bytes) {
        for (int offset = 0; offset < bytes.length; ++offset) {
            if (bytes[offset] == ExtensionActionUtil.UNIT_SEPARATOR) {
                return offset;
            }
        }
        return -1;
    }
}
