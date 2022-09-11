package org.opensearch.identity;

/** Authorization token from a http header */
public class HttpHeaderToken implements AuthenticationToken {
    public final static String HEADER_NAME = "Authorization";
    private final String headerValue;

    public HttpHeaderToken(final String headerValue) {
        this.headerValue = headerValue;
    }

    public String getHeaderValue() { 
        return headerValue;
    }
}
