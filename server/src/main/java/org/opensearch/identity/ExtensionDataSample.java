/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * On installation, each extension is added as a hidden user with pair <uniqueExtensionId, extensionSecret>
 *
 * This class lets you interact with the stored <extension, secret> pairs. 
 *
 * @opensearch.experimental
 */
public class ExtensionDataSample {


    static Map<String, byte[]> extensionMap = new HashMap<>();

    /**
     * Adds a new extension to the extensionMap; generates the extension's secret 
     * @param uniqueExtensionId: The PERMANENT identifier of a given extension
     */
    public static void addExtension(String uniqueExtensionId){
        
        byte[] salt = new byte[16];
        SecureRandom secRan = new SecureRandom();
        secRan.nextBytes(salt); // A random 16 value byte array 
        extensionMap.put(uniqueExtensionId, salt);
    }

    /**
     * @param uniqueExtensionId: The PERMANENT identifier of a given extension that was used on creation
     * @return The byte[] secret associated with the given extension -- do not share this with the extension 
     * */
    public static byte[] getExtensionSecret(String uniqueExtensionId){

        return extensionMap.get(uniqueExtensionId);

    }

    public int getExtensionCount() {

        return extensionMap.size();
    }

}
