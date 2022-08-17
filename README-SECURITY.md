# OpenSearch with Security

This is a prototype of how security can be added, this document serves as a tour of the features and defined the next stages of investigation.

## What is the basis for this variant?

There are several open-sourced security frameworks available, during an [initial pull request](https://github.com/opensearch-project/OpenSearch/pull/4028#issuecomment-1198385167) several came to light, Spring Security, Apache CXF, and Shiro. Shiro's focused design around authentication and authorization scenarios made it the candidate for this investigation.

## What scenarios are we trying to understand?

### How and where should identity information be captured?
Looking at the existing SecurityFilter implemenation of

### How identity information is accessed during a request?
### How and where can identity information be verified that aligns with the existing Task focused security model of the security plugin?
### How to apply identity information for internal processes?


## OpenSearch Scenarios

After starting opensearch with `./gradlew run` the console log will start capturing output, then the normal cluster operations can be performed.

### Cluster startup process
#### OpenSearch.log messages
```text
...
[2022-08-17T19:01:32,053][DEBUG][o.o.c.c.PublicationTransportHandler] [runTask-0] received full cluster state version [2] with size [284]
[2022-08-17T19:01:32,062][INFO ][o.o.t.TransportService   ] [runTask-0] Action: internal:cluster/coordination/commit_state, as Subject: INTERNAL-PreVoteCollector.java.start@193
```

### Cluster monitoring
### OpenSearch.log messages
```text
[2022-08-17T19:02:02,040][ERROR][o.o.a.a.c.n.s.TransportNodesStatsAction] [runTask-0] INTERNAL-NodeClient.java.executeLocally@115 is NOT allowed to cluster:monitor/nodes/stats, but is not being stopped
[2022-08-17T19:02:02,043][INFO ][o.o.t.TransportService   ] [runTask-0] Action: cluster:monitor/nodes/stats[n], as Subject: INTERNAL-NodeClient.java.executeLocally@115
[2022-08-17T19:02:02,045][ERROR][o.o.a.a.i.s.TransportIndicesStatsAction] [runTask-0] INTERNAL-NodeClient.java.executeLocally@115 is NOT allowed to indices:monitor/stats, but is not being stopped
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
[2022-08-17T19:08:28,729][INFO ][o.o.a.a.c.h.TransportClusterHealthAction] [runTask-0] admin is allowed to cluster:monitor/health
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
[2022-08-17T19:06:05,159][ERROR][o.o.a.a.c.h.TransportClusterHealthAction] [runTask-0] user is NOT allowed to cluster:monitor/health, but is not being stopped
```

### Action as non-user

#### Request command
`curl http://johny:appleseed@localhost:9200`

#### Response
```text
{"error":"Realm [org.opensearch.identity.MyShiroModule$MyRealm@d43e540] was unable to find account data for the submitted AuthenticationToken [org.apache.shiro.authc.UsernamePasswordToken - johny, rememberMe=false].","status":401}%
```

#### OpenSearch.log messages
No messages
