# Authentication Flow

## Authentication from incoming requests

As requests are received by an OpenSearch node they need to be authenticated.  Different modes can be supported such as Http Basic, Http Bearer, Kerberos, etc... these types of authentication are provided by the request source.  OpenSearch will translate these into an AuthenticationToken which can be feed into `Subject.login(token)` which will attempt to authenticate the user with the configured authentication providers.

![Authentication Flow](https://user-images.githubusercontent.com/2754967/202580793-9aab17e0-9645-4216-bcee-efddc932940a.PNG)

The subject is associated for the lifetime of the request on that node.  If the request will be sent to other nodes within the cluster, the authentication information will be attached with a generated access token no matter the AuthenticationToken that was used to authenticate the user.  By including expiry on these tokens the exposure of the subjects permissions are greatly limited.

![AccessToken for inter-node communication](https://user-images.githubusercontent.com/2754967/202580773-9b0ab15f-834c-45dc-9faf-48e6b832f85e.PNG)

## Ensuring Authentication throughout OpenSearch

As the OpenSearch service starts or in a running instance threads are created with the subject defaulting to Unauthenticated.  This is done to support backward compatibility as request authentication has never been a hard requirement of the service.

**Why partition state by threads start?**
Thread context is already widely used in OpenSearch to pass information through the transport layers.  This is not the most ideal state - and can be altered at a future time.  Many frameworks such as Shiro have static accessors for authentication information that is internally retrieved from a thread context.

Any risks to this pattern?  Absolutely, naively usage of thread local storage for authentication information can leak into reused threads.  This is an elevation of privileges and can be hard to spot during testing, but triggerable in production scenarios.

If we are authoring functionality to store Subject specific state, A) access information through stack / heap variables B) reuse known good libraries C) last resort - get a detailed review on the lifetime management of thread local storage objects

### Service Startup
As OpenSearch starts there are several management systems that trigger to discover/elect the cluster manager, monitor for node usage, snapshot, etc.  These management systems should have structured identities that ship out of the box and they are assumed directly.

- [ ] Discovering all of these systems and applying least-privilege defaults is the desired end state.  This protects the system by having different layers of access that are specialized
- [ ] As an intermediate step OpenSearch uses a ThreadPool [1] class with different pools named for different system functions.  Creating identities per pool and restricting them is a quick'n dirty way to identify them and start the initial blast radius reduction
- [ ] Defaulting to all non-generic pool threads are 'SYSTEM' creates a boundary between internal actions and externally triggered actions.

[1] ThreadPool https://github.dev/peternied/OpenSearch-1/blob/d3b60b5362bf1c37ea94a569357aca7d8fb6352d/server/src/main/java/org/opensearch/threadpool/ThreadPool.java#L1

### Thread Start
All threads are managed through the OpenSearchThreadFactory [1].  By making sure this is the only place that thread can come from we can be sure that the Subject is always created as expected before any other actions take place.

There are two potential exception to this, the 'main' thread and 'ForkJoinPool' [2]

[ ] Confirmation that `new Thread(...)` is only called from the ThreadFactory
[ ] Figure out how to enforce defaults on 'main' thread and 'ForkJoinPool'
[ ] Research, are plugins able to create their own thread factories and bypass any requirements added to them.  This might not be solvable, but we should document the limitations.

[1] OpenSearchThreadFactory https://github.com/peternied/OpenSearch-1/blob/d3b60b5362bf1c37ea94a569357aca7d8fb6352d/server/src/main/java/org/opensearch/common/util/concurrent/OpenSearchExecutors.java#L390
[2] ForkJoinPool https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html
