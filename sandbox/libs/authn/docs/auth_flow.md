# Authentication Flow

## Authentication from incoming requests

As requests are received by an OpenSearch node they need to be authenticated.  Different modes can be supported such as Http Basic, Http Bearer, Kerberos, etc... these types of authentication are provided by the request source.  OpenSearch will translate these into an AuthenticationToken which can be feed into `Subject.login(token)` which will attempt to authenticate the user with the configured authentication providers.

![Authentication Flow](https://user-images.githubusercontent.com/2754967/202580793-9aab17e0-9645-4216-bcee-efddc932940a.PNG)

The subject is associated for the lifetime of the request on that node.  If the request will be sent to other nodes within the cluster, the authentication information will be attached with a generated access token no matter the AuthenticationToken that was used to authenticate the user.  By including expiry on these tokens the exposure of the subjects permissions are greatly limited. 

![AccessToken for inter-node communication](https://user-images.githubusercontent.com/2754967/202580773-9b0ab15f-834c-45dc-9faf-48e6b832f85e.PNG)
