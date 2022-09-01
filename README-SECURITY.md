# OpenSearch with Security

This is a prototype of how security can be added, this document serves as a tour of the features and defined the next stages of investigation.

_Terminology:_
* Subject: An individual, process, or device that causes information to flow among objects or change to the system state.
* Principal: An application-wide identifier that is persisted

## What is the basis for this variant?

There are several open-sourced security frameworks available, during an [initial pull request](https://github.com/opensearch-project/OpenSearch/pull/4028#issuecomment-1198385167) several came to light, Spring Security, Apache CXF, and Shiro. Shiro's focused design around authentication and authorization scenarios made it the candidate for this investigation.

## Scenarios covered in this prototype

### Authentication is governed by org.opensearch.idenity interfaces

* AuthenticationManager
  Authentication management for OpenSearch. Retrieve the current subject, switch to a subject, and persist subject identity through request lifetime

* Subject
  An individual, process, or device that causes information to flow among objects or change to the system state.  Used to authorization actions inside of the OpenSearch ecosystem.


### How and where should Subject information be captured?
From external-in perspective suggests that the RestController has access to request headers that could contain this information, and it could be picked up before the request is dispatched.  This also would be a good point to dispatch to a login workflow if challenge auth is supported.

#### Internal subjects

Internally subjects are accessible via `dangerousAuthenticateAs(...)` method on the AuthorizationManager, this will work for stream of execution workflows.  For workflows using specific threads more investigations will be needed

##### Open Question - How to handle internal listeners / thread pools?
When investigating the InternalClusterInfoService, using `dangerousAuthenticateAs(...)` on `refresh()` does not properly encapsulate the lifetime since the ClusterStateListener can be involved via clusterChanged or from lister invocation.  While the building blocks for the authenticationAs and associateWith can support this scenario. Closer study of this class or similar patterns will be needed to ensure proper encapsulation of subjects. 

#### Open Question - how to track the Subject inside of plugin?
Initial consideration is that adding parameters to `associateWith` that allows for inserting a plugin principle into the principal chain.  Within the permissions evaluation system, both the user principal and plugin principals need to have the permissions for the action.  

#### Open Question - how to track the Subject inside of extension?
See design proposal in https://github.com/opensearch-project/OpenSearch/pull/4299

#### Open Question - how should we determine the granularity of a given internal OpenSearch Subject?

### How Subject information is accessed during a request?
Looking at the existing [SecurityFilter](https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/filter/SecurityFilter.java) implementation of the security plugin, this was the point in time when AuthZ was verifiable, which pertained to the Task filter.

#### Open Question - For plugins, should this be expected to be added into the thread context?

### How and where can Subject information be verified that aligns with the existing Task focused security model of the security plugin?
`this.authenticationManager.getCurrentSubject().isPermitted(...)` is accessible anywhere within the context of the thread running the request.  While typically checking AuthZ should be done as soon as possible to eliminate wasted processing time, it means layers of auth can be added.  E.g. First check that the subject can execute the task by name.  Then as indexes are being resolved from an index pattern block/filter based on the subjects available view.  The existing security plugin has a [IndexResolverReplacer](https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/resolver/IndexResolverReplacer.java) that emulates OpenSearch's behavior where this _could_ be done inline.

A check was added in TransportAction to ensure that all requests are authenticated or they are errored out, then logging of the resolved permissions.

### How to apply Subject information for internal processes?
AuthenticationManager can be used to get and pass authentication information around OpenSearch execution environments, getCurrentSubject() provides the subject that can be checked for permissions.

## OpenSearch Scenarios

After starting opensearch with `./gradlew run` the console log will start capturing output, then the normal cluster operations can be performed.

### Cluster startup process
#### OpenSearch.log messages
```text
...

[2022-09-01T20:33:24,238][DEBUG][o.o.c.c.PublicationTransportHandler] [runTask-0] received full cluster state version [2] with size [282]
[2022-09-01T20:33:24,245][INFO ][o.o.t.TransportService   ] [runTask-0] Action: internal:cluster/coordination/commit_state, as Subject: INTERNAL-PreVoteCollector
```

### Cluster monitoring
### OpenSearch.log messages
```text
[2022-09-01T21:29:03,326][ERROR][o.o.a.a.c.n.s.TransportNodesStatsAction] [runTask-0] Unable to permit user 'INTERNAL-refreshNodeStats', for permission cluster:monitor/nodes/stats, but is not being stopped
[2022-09-01T21:29:03,327][INFO ][o.o.t.TransportService   ] [runTask-0] Action: cluster:monitor/nodes/stats[n], as Subject: INTERNAL-refreshNodeStats
[2022-09-01T21:29:03,327][ERROR][o.o.a.a.i.s.TransportIndicesStatsAction] [runTask-0] Unable to permit user 'INTERNAL-refreshNodeStats', for permission indices:monitor/stats, but is not being stopped
```

### Action as admin

#### Request command
`curl http://admin:admin@localhost:9200/_cat/health`

#### Response
```text
1660763308 19:08:28 runTask green 1 1 true 0 0 0 0 0 0 - 100.0%
```

#### OpenSearch.log messages
```text
[2022-09-01T21:28:34,411][INFO ][o.o.i.ShiroAuthenticationManager] [runTask-0] Authenticated user 'admin'
[2022-09-01T21:28:34,415][INFO ][o.o.a.a.c.h.TransportClusterHealthAction] [runTask-0] admin is allowed to cluster:monitor/health
```

### Action as user

#### Request command
`curl http://user:user@localhost:9200/_cat/health`

#### Response
```text
1660763165 19:06:05 runTask green 1 1 true 0 0 0 0 0 0 - 100.0%
```

#### OpenSearch.log messages
```text
[2022-09-01T21:28:39,623][INFO ][o.o.i.ShiroAuthenticationManager] [runTask-0] Authenticated user 'user'
[2022-09-01T21:28:39,624][ERROR][o.o.a.a.c.h.TransportClusterHealthAction] [runTask-0] Unable to permit user 'user', for permission cluster:monitor/health, but is not being stopped
```

### Action as non-user

#### Request command
`curl http://johny:appleseed@localhost:9200`

#### Response
```text
{"error":"Realm [org.opensearch.identity.MyShiroModule$MyRealm@13d52257] was unable to find account data for the submitted AuthenticationToken [org.apache.shiro.authc.UsernamePasswordToken - johny, rememberMe=false].","status":401}%
```

#### OpenSearch.log messages
No messages
