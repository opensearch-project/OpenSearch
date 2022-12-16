/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import com.fasterxml.jackson.databind.InjectableValues;
import org.opensearch.SpecialPermission;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class NonValidatingObjectMapper {
    private static final ObjectMapper nonValidatingObjectMapper = new ObjectMapper();

    static {
        nonValidatingObjectMapper.setSerializationInclusion(Include.NON_NULL);
        nonValidatingObjectMapper.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, false);
        nonValidatingObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        nonValidatingObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    }

    public static void inject(final InjectableValues.Std injectableValues) {
        nonValidatingObjectMapper.setInjectableValues(injectableValues);
    }

    public static <T> T readValue(String string, JavaType jt) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return nonValidatingObjectMapper.readValue(string, jt);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static TypeFactory getTypeFactory() {
        return nonValidatingObjectMapper.getTypeFactory();
    }

}
