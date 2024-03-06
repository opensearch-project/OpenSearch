/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.utils;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to parse information from the thread context
 */
public final class ThreadContextParser {

    private ThreadContextParser() {}

    /**
     * Get User info from the thread context
     *
     * @param threadContext context of the thread
     * @return Map of {@link Attribute} and the corresponding values
     */
    public static Map<Attribute, Object> getUserInfoFromThreadContext(ThreadContext threadContext) {
        Map<Attribute, Object> userInfoMap = new HashMap<>();
        if (threadContext == null) {
            return userInfoMap;
        }
        Object userInfoObj = threadContext.getTransient(QueryInsightsSettings.REQUEST_HEADER_USER_INFO);
        if (userInfoObj == null) {
            return userInfoMap;
        }
        String userInfoStr = userInfoObj.toString();
        Object remoteAddressObj = threadContext.getTransient(QueryInsightsSettings.REQUEST_HEADER_REMOTE_ADDRESS);
        if (remoteAddressObj != null) {
            userInfoMap.put(Attribute.REMOTE_ADDRESS, remoteAddressObj.toString());
        }

        String[] userInfo = userInfoStr.split("\\|");
        if ((userInfo.length == 0) || (Strings.isNullOrEmpty(userInfo[0]))) {
            return userInfoMap;
        }
        userInfoMap.put(Attribute.USER_NAME, userInfo[0].trim());
        if ((userInfo.length > 1) && !Strings.isNullOrEmpty(userInfo[1])) {
            userInfoMap.put(Attribute.USER_BACKEND_ROLES, Arrays.asList(userInfo[1].split(",")));
        }
        if ((userInfo.length > 2) && !Strings.isNullOrEmpty(userInfo[2])) {
            userInfoMap.put(Attribute.USER_ROLES, Arrays.asList(userInfo[2].split(",")));
        }
        if ((userInfo.length > 3) && !Strings.isNullOrEmpty(userInfo[3])) {
            userInfoMap.put(Attribute.USER_TENANT, userInfo[3].trim());
        }
        return userInfoMap;
    }
}
