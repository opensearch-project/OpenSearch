/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels.rules;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Rules to get user info labels, specifically, the info that is injected by the security plugin.
 */
public class DefaultUserInfoLabelingRule implements Rule {
    /**
     * Constant setting for user info header key that are injected during authentication
     */
    private final String REQUEST_HEADER_USER_INFO = "_opendistro_security_user_info";
    /**
     * Constant setting for remote address info header key that are injected during authentication
     */
    private final String REQUEST_HEADER_REMOTE_ADDRESS = "_opendistro_security_remote_address";

    public static final String REMOTE_ADDRESS = "remote_address";
    public static final String USER_NAME = "user_name";
    public static final String USER_BACKEND_ROLES = "user_backend_roles";
    public static final String USER_ROLES = "user_roles";
    public static final String USER_TENANT = "user_tenant";

    /**
     * @param threadContext
     * @param searchRequest
     * @return Map of User related info and the corresponding values
     */
    @Override
    public Map<String, Object> evaluate(ThreadContext threadContext, SearchRequest searchRequest) {
        return getUserInfoFromThreadContext(threadContext);
    }

    /**
     * Get User info, specifically injected by the security plugin, from the thread context
     *
     * @param threadContext context of the thread
     * @return Map of User related info and the corresponding values
     */
    private Map<String, Object> getUserInfoFromThreadContext(ThreadContext threadContext) {
        Map<String, Object> userInfoMap = new HashMap<>();
        if (threadContext == null) {
            return userInfoMap;
        }
        Object remoteAddressObj = threadContext.getTransient(REQUEST_HEADER_REMOTE_ADDRESS);
        if (remoteAddressObj != null) {
            userInfoMap.put(REMOTE_ADDRESS, remoteAddressObj.toString());
        }

        Object userInfoObj = threadContext.getTransient(REQUEST_HEADER_USER_INFO);
        if (userInfoObj == null) {
            return userInfoMap;
        }
        String userInfoStr = userInfoObj.toString();
        String[] userInfo = userInfoStr.split("\\|");
        if ((userInfo.length == 0) || (Strings.isNullOrEmpty(userInfo[0]))) {
            return userInfoMap;
        }
        userInfoMap.put(USER_NAME, userInfo[0].trim());
        if ((userInfo.length > 1) && !Strings.isNullOrEmpty(userInfo[1])) {
            userInfoMap.put(USER_BACKEND_ROLES, Arrays.asList(userInfo[1].split(",")));
        }
        if ((userInfo.length > 2) && !Strings.isNullOrEmpty(userInfo[2])) {
            userInfoMap.put(USER_ROLES, Arrays.asList(userInfo[2].split(",")));
        }
        if ((userInfo.length > 3) && !Strings.isNullOrEmpty(userInfo[3])) {
            userInfoMap.put(USER_TENANT, userInfo[3].trim());
        }
        return userInfoMap;
    }
}
