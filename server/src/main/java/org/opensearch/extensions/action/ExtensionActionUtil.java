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
import org.opensearch.common.io.stream.StreamInput;

import java.io.ByteArrayOutputStream;
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
     * @param  requestBytes is a byte array containing the request data, used by the "createActionRequest"
     * method to create an "ActionRequest" object, which represents the request model to be processed on the server.
     * @return an "Action Request" object, which represents the request model for processing on the server,
     * and is created using the request data stored in the byte array provided in the "requestBytes" parameter.
     */
    public static ActionRequest createActionRequest(ExtensionActionRequest requestBytes) {
        int nullPos = delimPos(requestBytes.getRequestBytes());
        String requestClassName = new String(Arrays.copyOfRange(requestBytes.getRequestBytes(), 0, nullPos + 1), StandardCharsets.UTF_8)
            .stripTrailing();
        ActionRequest actionRequest = null;
        try {
            Class<?> clazz = Class.forName(requestClassName);
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(
                Arrays.copyOfRange(requestBytes.getRequestBytes(), nullPos + 1, requestBytes.getRequestBytes().length)
            );
            actionRequest = (ActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.debug("No request class [" + requestClassName + "] is available: " + e.getMessage());
        }
        return actionRequest;
    }

    /**
     * @param actionRequest  is an instance of the Action Request class that is used to create an ExtensionActionRequest object.
     * @return an Extension ActionRequest object based on an Action Request input.
     */
    public static ExtensionActionRequest createExtensionActionRequest(ActionRequest actionRequest) {
        byte[] requestBytes;
        ExtensionActionRequest extensionActionRequest = null;
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            requestBytes = outputStream.toByteArray();
            String requestClassName = new String(Arrays.copyOfRange(requestBytes, 0, delimPos(requestBytes) + 1), StandardCharsets.UTF_8)
                .stripTrailing();
            Class<?> clazz = Class.forName(requestClassName);
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(
                Arrays.copyOfRange(requestBytes, delimPos(requestBytes) + 1, requestBytes.length)
            );
            extensionActionRequest = (ExtensionActionRequest) constructor.newInstance(requestByteStream);
        } catch (Exception e) {
            logger.debug("Failed to create request bytes for [" + actionRequest + "]: " + e.getMessage());
        }
        return extensionActionRequest;
    }

    private static int delimPos(byte[] bytes) {
        for (int offset = 0; offset < bytes.length; ++offset) {
            if (bytes[offset] == ExtensionActionUtil.UNIT_SEPARATOR) {
                return offset;
            }
        }
        return -1;
    }
}
