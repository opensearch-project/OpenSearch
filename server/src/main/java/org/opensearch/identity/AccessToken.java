package org.opensearch.identity;

import org.joda.time.DateTime;
import org.opensearch.common.io.stream.NamedWriteable;

/**
 * Tamperproof encapsulation the identity of a subject
 * 
 * @opensearch.experimental
 */
public class AccessToken implements AuthenticationToken {

    /** Actual implementation would be something like:
    
    public String accessToken;
    public DateTime accessTokenExpiration;
    public String refreshToken;
    public DateTime refreshTokenExpiration; 
    
      */

}
