/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.common.settings.Settings;
import org.opensearch.identity.ConfigConstants;

public final class SecurityUtils {

    protected final static Logger log = LogManager.getLogger(SecurityUtils.class);
    private static final String ENV_PATTERN_SUFFIX = "\\.([\\w=():\\-_]+?)(\\:\\-[\\w=():\\-_]*)?\\}";
    static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{env" + ENV_PATTERN_SUFFIX);
    static final Pattern ENVBC_PATTERN = Pattern.compile("\\$\\{envbc" + ENV_PATTERN_SUFFIX);
    static final Pattern ENVBASE64_PATTERN = Pattern.compile("\\$\\{envbase64" + ENV_PATTERN_SUFFIX);
    public static Locale EN_Locale = forEN();


    private SecurityUtils() {
    }

    //https://github.com/tonywasher/bc-java/commit/ee160e16aa7fc71330907067c5470e9bf3e6c383
    //The Legion of the Bouncy Castle Inc
    private static Locale forEN()
    {
        if ("en".equalsIgnoreCase(Locale.getDefault().getLanguage()))
        {
            return Locale.getDefault();
        }

        Locale[] locales = Locale.getAvailableLocales();
        for (int i = 0; i != locales.length; i++)
        {
            if ("en".equalsIgnoreCase(locales[i].getLanguage()))
            {
                return locales[i];
            }
        }

        return Locale.getDefault();
    }

    public static String evalMap(final Map<String, Set<String>> map, final String index) {

        if (map == null) {
            return null;
        }

        //TODO: check what to do with _all
        /*if (map.get(index) != null) {
            return index;
        } else if (map.get("*") != null) {
            return "*";
        }
        if (map.get("_all") != null) {
            return "_all";
        }*/

        return map.keySet().stream()
            .filter(key -> WildcardMatcher.from(key).test(index))
            .findAny()
            .orElse(null);
    }

    @SafeVarargs
    public static <T> Map<T, T>  mapFromArray(T ... keyValues) {
        if(keyValues == null) {
            return Collections.emptyMap();
        }
        if (keyValues.length % 2 != 0) {
            log.error("Expected even number of key/value pairs, got {}.", Arrays.toString(keyValues));
            return null;
        }
        Map<T, T> map = new HashMap<>();

        for(int i = 0; i<keyValues.length; i+=2) {
            map.put(keyValues[i], keyValues[i+1]);
        }
        return map;
    }

    public static String replaceEnvVars(String in, Settings settings) {
        if(in == null || in.isEmpty()) {
            return in;
        }

        if(settings == null || settings.getAsBoolean(ConfigConstants.IDENTITY_DISABLE_ENVVAR_REPLACEMENT, false)) {
            return in;
        }

        return replaceEnvVarsBC(replaceEnvVarsNonBC(replaceEnvVarsBase64(in)));
    }

    private static String replaceEnvVarsNonBC(String in) {
        //${env.MY_ENV_VAR}
        //${env.MY_ENV_VAR:-default}
        Matcher matcher = ENV_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), false);
            if(replacement != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String replaceEnvVarsBC(String in) {
        //${envbc.MY_ENV_VAR}
        //${envbc.MY_ENV_VAR:-default}
        Matcher matcher = ENVBC_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), true);
            if(replacement != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String replaceEnvVarsBase64(String in) {
        //${envbc.MY_ENV_VAR}
        //${envbc.MY_ENV_VAR:-default}
        Matcher matcher = ENVBASE64_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), false);
            if(replacement != null) {
                matcher.appendReplacement(sb, (Matcher.quoteReplacement(new String(Base64.getDecoder().decode(replacement), StandardCharsets.UTF_8))));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    //${env.MY_ENV_VAR}
    //${env.MY_ENV_VAR:-default}
    private static String resolveEnvVar(String envVarName, String mode, boolean bc) {
        final String envVarValue = System.getenv(envVarName);
        if(envVarValue == null || envVarValue.isEmpty()) {
            if(mode != null && mode.startsWith(":-") && mode.length() > 2) {
                return bc?Hasher.hash(mode.substring(2).toCharArray()):mode.substring(2);
            } else {
                return null;
            }
        } else {
            return bc?Hasher.hash(envVarValue.toCharArray()):envVarValue;
        }
    }
}

