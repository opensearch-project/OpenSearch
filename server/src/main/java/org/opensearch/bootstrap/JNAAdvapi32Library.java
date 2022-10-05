/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.Structure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.util.List;

/**
 * Library for Windows/Advapi32
 *
 * @opensearch.internal
 */
final class JNAAdvapi32Library {

    private static final Logger logger = LogManager.getLogger(JNAAdvapi32Library.class);

    private static final class Holder {
        private static final JNAAdvapi32Library instance = new JNAAdvapi32Library();
    }

    private JNAAdvapi32Library() {
        if (Constants.WINDOWS) {
            try {
                Native.register("advapi32");
                logger.debug("windows/Advapi32 library loaded");
            } catch (NoClassDefFoundError e) {
                logger.warn("JNA not found. native methods and handlers will be disabled.");
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link Windows/Advapi32 library. native methods and handlers will be disabled.");
            }
        }
    }

    static JNAAdvapi32Library getInstance() {
        return Holder.instance;
    }

    /**
     * Access right required to query an access token.
     * Used by {@link #OpenProcessToken(Pointer, int, PointerByReference)}.
     *
     * https://learn.microsoft.com/en-us/windows/win32/secauthz/access-rights-for-access-token-objects
     */
    public static final int TOKEN_QUERY = 0x0008;

    /**
     * TOKEN_INFORMATION_CLASS enumeration value that specifies the type of information being assigned to or retrieved from an access token.
     * Used by {@link #GetTokenInformation(Pointer, int, Structure, int, IntByReference)}.
     *
     * https://learn.microsoft.com/en-us/windows/win32/api/winnt/ne-winnt-token_information_class
     */
    public static final int TOKEN_ELEVATION = 0x14;

    /**
     * Native call to the Advapi32 API to open the access token associated with a process.
     *
     * @param processHandle Handle to the process whose access token is opened.
     *            The process must have the PROCESS_QUERY_INFORMATION access permission.
     * @param desiredAccess Specifies an access mask that specifies the requested types of access to the access token.
     *            These requested access types are compared with the discretionary access control list (DACL) of the token to determine which accesses are granted or denied.
     * @param tokenHandle Pointer to a handle that identifies the newly opened access token when the function returns.
     * @return If the function succeeds, the return value is true.
     *         If the function fails, the return value is false.
     *         To get extended error information, call GetLastError.
     */
    native boolean OpenProcessToken(Pointer processHandle, int desiredAccess, PointerByReference tokenHandle);

    /**
     * Retrieves a specified type of information about an access token.
     * The calling process must have appropriate access rights to obtain the information.
     *
     * @param tokenHandle Handle to an access token from which information is retrieved.
     *            If TokenInformationClass specifies TokenSource, the handle must have TOKEN_QUERY_SOURCE access.
     *            For all other TokenInformationClass values, the handle must have TOKEN_QUERY access.
     * @param tokenInformationClass Specifies a value from the TOKEN_INFORMATION_CLASS enumerated type to identify the type of information the function retrieves.
     * @param tokenInformation Pointer to a buffer the function fills with the requested information.
     *            The structure put into this buffer depends upon the type of information specified by the TokenInformationClass parameter.
     * @param tokenInformationLength Specifies the size, in bytes, of the buffer pointed to by the TokenInformation parameter.
     *            If TokenInformation is NULL, this parameter must be zero.
     * @param returnLength Pointer to a variable that receives the number of bytes needed for the buffer pointed to by the TokenInformation parameter.
     *            If this value is larger than the value specified in the TokenInformationLength parameter, the function fails and stores no data in the buffer.
     * @return If the function succeeds, the return value is true.
     *         If the function fails, the return value is zero.
     *         To get extended error information, call GetLastError.
     */
    native boolean GetTokenInformation(
        Pointer tokenHandle,
        int tokenInformationClass,
        Structure tokenInformation,
        int tokenInformationLength,
        IntByReference returnLength
    );

    /**
     * The TOKEN_ELEVATION structure indicates whether a token has elevated privileges.
     *
     * https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-token_elevation
     */
    public static class TokenElevation extends Structure {
        /**
         * A nonzero value if the token has elevated privileges; otherwise, a zero value.
         */
        public int TokenIsElevated;

        @Override
        protected List<String> getFieldOrder() {
            return List.of("TokenIsElevated");
        }
    }
}
