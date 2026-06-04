/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * ExtensionActionUtil - a class for creating and processing remote requests using byte arrays.
 */
public class ExtensionActionUtil {

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
     * @throws  RuntimeException If a RuntimeException occurs while creating the proxy request bytes.
     */
    public static byte[] createProxyRequestBytes(ActionRequest request) throws RuntimeException {
        byte[] requestClassBytes = request.getClass().getName().getBytes(StandardCharsets.UTF_8);
        byte[] requestBytes;

        try {
            requestBytes = convertParamsToBytes(request);
            assert requestBytes != null;
            return ByteBuffer.allocate(requestClassBytes.length + 1 + requestBytes.length)
                .put(requestClassBytes)
                .put(UNIT_SEPARATOR)
                .put(requestBytes)
                .array();
        } catch (RuntimeException e) {
            throw new RuntimeException("RuntimeException occurred while creating proxyRequestBytes");
        }
    }

    /**
     * @param  requestBytes is a byte array containing the request data, used by the "createActionRequest"
     * method to create an "ActionRequest" object, which represents the request model to be processed on the server.
     * @return an "Action Request" object representing the request model for processing on the server,
     * or {@code null} if the request data is invalid or null.
     * @throws ReflectiveOperationException if an exception occurs during the reflective operation, such as when
     *  resolving the request class, accessing the constructor, or creating an instance  using reflection
     * @throws NullPointerException if a null pointer exception occurs during the creation of the ActionRequest object
     */
    public static ActionRequest createActionRequest(byte[] requestBytes) throws ReflectiveOperationException {
        int delimPos = delimPos(requestBytes);
        String requestClassName = new String(Arrays.copyOfRange(requestBytes, 0, delimPos + 1), StandardCharsets.UTF_8).stripTrailing();
        try {
            Class<?> clazz = Class.forName(requestClassName);
            Constructor<?> constructor = clazz.getConstructor(StreamInput.class);
            StreamInput requestByteStream = StreamInput.wrap(Arrays.copyOfRange(requestBytes, delimPos + 1, requestBytes.length));
            return (ActionRequest) constructor.newInstance(requestByteStream);
        } catch (ReflectiveOperationException e) {
            throw new ReflectiveOperationException(
                "ReflectiveOperationException occurred while creating extensionAction request from bytes",
                e
            );
        } catch (NullPointerException e) {
            throw new NullPointerException(
                "NullPointerException occurred while creating extensionAction request from bytes" + e.getMessage()
            );
        }
    }

    /**
     * Converts the given object of type T, which implements the {@link Writeable} interface, to a byte array.
     * @param <T> the type of the object to be converted to bytes, which must implement the {@link Writeable} interface.
     * @param writeableObject the object of type T to be converted to bytes.
     * @return a byte array containing the serialized bytes of the given object, or {@code null} if the input is invalid or null.
     * @throws IllegalStateException if a failure occurs while writing the data
     */
    public static <T extends Writeable> byte[] convertParamsToBytes(T writeableObject) throws IllegalStateException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeableObject.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        } catch (IOException ieo) {
            throw new IllegalStateException("Failure writing bytes", ieo);
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
