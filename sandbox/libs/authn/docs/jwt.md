# JWTs in OpenSearch

JWTs are one of the current methods OpenSearch uses to authenticate Rest requests to an OpenSearch cluster.

When the `jwt_auth_domain` is configured in the `authc` section of the security configuration file, the security plugin will verify the JWT with the `signing_key` configured and decrypt the payload to extract `subject` and `roles` information from the claims in the payload of the JWT.

JWTs are mainly generated in the following 3 ways:

1) Generated after first authentication with a SAML server and used in subsequent requests to authenticate the user to prevent rounttrips to the SAML identity provider
    - For SAML authentication, the security plugin takes the SAML response and extracts `claims` from the response to embed in the JWT. The `exchange_key` is the key used to sign the token. The key is created by the security plugin.
    - A separate `jwt_auth_domain` does not need to be configured when `saml_auth_domain` is in the list of authentication backends
2) After authentication with an OIDC IdP the security plugin gets a JWT from the JSON web key set (JWKS) endpoint of the IdP
    - Keys or shared secrets do not need to be configured in security `config.yml` because these retrieved from the IdP.
3) JWT Auth domain - Using a shared signing key - A separate authentication server that can authenticate and create JWTs is compatible with the JSON Auth domain for the security plugin. The payload of the jwt needs to minimally contain a subject (keyed by `subject_key`) and roles (keyed by `roles_key`)


These JWTs are utilized by using an HTTP Authorization header like so:

`curl -XGET -H "Authorization: Bearer ${ACCESS_TOKEN}" http://localhost:9200`

This is often referred to as Bearer Authentication

# Tokens for Delegated Authority

The following is a proposal for how to best secure asynchronous tasks running in OpenSearch or OpenSearch extensions.

When a task or job is run in OpenSearch, the executor of the task should be explicitly defined. By explicitly specifying the subject context for a job, it makes
it clearer to understand what privileges are in use when the job is executing and gives an administrator the ability to fine tune the security model for their cluster.
In addition to offering better fine tuning for security, it also gives administrators the power to revoke privileges to prevent an errant job from executing.

In order to delegate authority for a background task or job, I propose to create a token vending service to produce tokens that confer access to the cluster and
let's the background task assume a subject when executing.

There are 2 types of tokens:

- Refresh Token - A Refresh Token is longer lasting and used to receive short-lived Access Tokens that actually confer access
- Access Token - Access Tokens are short-lived tokens that confer access

Creation of an asynchronous job or task should require a Refresh Token on creation which is associated with a subject and let's the job run with the subject's permissions. When the task
starts execution it will use the refresh token to obtain an access token which will allow the task to interact with the cluster as the subject who the token was created for.

Refresh tokens will be associated with a subject (and an extension?) and both the subject and administrators (or subjects with requisite permissions to revoke tokens of others) will be able to revoke access to the tokens. Issuing a token for oneself or on behalf of others will be another set of permissions that can be assigned.

# Internal Cluster Actions

Opensearch is a distributed search engine composed of nodes of different roles. When a client makes a request to a cluster, the request is serviced by one or many nodes. A good example of this is the cluster health action (`cluster:monitor/health`) that runs actions on all nodes of the cluster to perform the health check. In Opensearch with the Security plugin, a user is authenticated at the first node that handles the request, the ThreadContext is populated and subsequent actions on other nodes reference the user info saved on the ThreadContext. The ThreadContext is transmitted in the cluster using the InboundHandler and OutboundHandler. 

Internal Cluster Requests are authenticated using the thread context. See below for an example:

```
public class HeaderHelper {

    public static boolean isInterClusterRequest(final ThreadContext context) {
        return context.getTransient(ConfigConstants.OPENDISTRO_SECURITY_SSL_TRANSPORT_INTERCLUSTER_REQUEST) == Boolean.TRUE;
    }

    public static boolean isDirectRequest(final ThreadContext context) {
        
        return  "direct".equals(context.getTransient(ConfigConstants.OPENDISTRO_SECURITY_CHANNEL_TYPE))
                  || context.getTransient(ConfigConstants.OPENDISTRO_SECURITY_CHANNEL_TYPE) == null;
    }

    ...
}
final boolean interClusterRequest = HeaderHelper.isInterClusterRequest(threadContext);

final boolean internalRequest =
    (interClusterRequest || HeaderHelper.isDirectRequest(threadContext))
    && action.startsWith("internal:")
    && !action.startsWith("internal:transport/proxy");
```

Internal actions can proceed through the chain without going through privilege evaluation on every node. 

To minimize the usage of ThreadContext, tokens can be used to transmit subject information from node-to-node to enable authorization to be performed before an action is executed on any node in the cluster.