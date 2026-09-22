------------------------------ MODULE FenceAutoRestore ------------------------------
(***************************************************************************)
(* The auto-restore trigger for a zero-replica remote-store-backed         *)
(* primary: when the node holding the only copy drops out of the cluster   *)
(* manager's view, the cluster manager re-points the shard at a            *)
(* remote-store recovery source and allocates it to a live node, instead   *)
(* of parking it at NO_VALID_SHARD_COPY (RED) waiting for a node that may  *)
(* never return. RFC #22768 Phase 1; the fence (Phase 0) is the safety     *)
(* prerequisite this module makes precise.                                 *)
(*                                                                          *)
(* What is modeled here is the CLUSTER-STATE surface, not the fence's      *)
(* internals. The sibling modules prove the fence's own guarantees over    *)
(* complete state spaces - seal ordering and the ack pipeline              *)
(* (RemoteStoreFence.tla), deterministic cross-term takeover               *)
(* (FenceTakeover.tla) - so the fence appears here as an abstract          *)
(* strictly-greater-term seal plus an owner-gated acknowledgement,         *)
(* which is exactly the interface the trigger consumes.                    *)
(*                                                                          *)
(* The actors and their races:                                             *)
(*  - The CLUSTER MANAGER applies atomic state updates: it observes a      *)
(*    node-left (a VIEW change - the departed writer may still be alive    *)
(*    behind a partition and still able to reach the object store),        *)
(*    fires the trigger (re-point + allocate + in-sync reset, one atomic   *)
(*    update, matching PrimaryShardAllocator/IndexMetadataUpdater), and    *)
(*    applies shard-started.                                               *)
(*  - The DEPARTED WRITER keeps acknowledging writes until the fence       *)
(*    refuses it. Nothing about the trigger waits for it to actually       *)
(*    stop; that is the point.                                             *)
(*  - The RESTORE TARGET seals at a strictly higher term, reads its        *)
(*    restore point after the seal, hydrates, and serves. It can die       *)
(*    mid-restore, and the trigger must be re-fireable at a yet-higher     *)
(*    term.                                                                *)
(*  - The departed node can REJOIN: before the trigger fired, its copy is  *)
(*    legitimately re-allocated (normal existing-store recovery); after,   *)
(*    its copy is stale (allocation id no longer in-sync) and is dropped.  *)
(*                                                                          *)
(* The FENCED constant is the load-bearing gate. TRUE is the shipped       *)
(* coupling - auto-restore requires index.remote_store.fencing.enabled -   *)
(* and every invariant holds. FALSE models firing the trigger on an        *)
(* unfenced index: the partitioned old primary keeps acknowledging writes  *)
(* after the restore target read its restore point, and TLC refutes        *)
(* NoAckedWriteLoss - the formal justification for gating the trigger on   *)
(* fencing rather than shipping it standalone.                             *)
(*                                                                          *)
(* Health is a derived label, not a proved property: an in-flight          *)
(* auto-restore is INITIALIZING with a remote-store recovery source, and   *)
(* the code change that reports it YELLOW rather than RED                  *)
(* (ClusterShardHealth#getInactivePrimaryHealth) is documented by the      *)
(* Health operator below. What TLC proves about availability is the        *)
(* liveness property: an unassigned shard with a live node eventually has  *)
(* a serving primary again - the shard does not rest in RED.               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
  Writers,      \* shard-copy incarnations (allocation ids), e.g. {w1, w2, w3}
  Nodes,        \* data nodes, e.g. {n1, n2}
  NoWriter,     \* model value: no writer
  MaxOps,       \* bound on acknowledged operations (state-space bound only)
  MaxTerm,      \* bound on primary terms (state-space bound only)
  FENCED        \* TRUE: acks are gated by the fence chain (the shipped
                \* coupling). FALSE: the refuted ungated trigger.

ASSUME NoWriter \notin Writers
ASSUME FENCED \in BOOLEAN

VARIABLES
  live,        \* [Nodes -> BOOLEAN] - the cluster manager's membership VIEW
  wNode,       \* [Writers -> Nodes] - where each writer runs
  wState,      \* [Writers -> {"unborn","serving","restoring","stopped"}]
  wTerm,       \* [Writers -> Nat] - the term each writer was appointed at
  hasRead,     \* [Writers -> BOOLEAN] - restore point read (post-seal)
  restore,     \* [Writers -> SUBSET (1..MaxOps)] - the restore point read
  (* The abstract fence: highest sealed term and the owner whose acks the  *)
  (* chain currently admits. Seal requires a strictly greater term - the   *)
  (* deterministic-takeover property FenceTakeover.tla establishes.        *)
  fenceTerm, fenceOwner,
  (* The cluster manager's routing view of the one shard.                  *)
  rState,      \* "assigned" | "unassigned_existing" | "initializing_remote"
  rWriter,     \* the writer the routing entry names (NoWriter when unassigned)
  inSync,      \* SUBSET Writers - the in-sync allocation ids
  delayOpen,   \* the node-left grace window (index.unassigned.node_left.
               \* delayed_timeout): TRUE while the delay marker set by
               \* disassociateDeadNodes is live on the unassigned primary
  (* Global acknowledgement history.                                       *)
  acked, ackedBy, nextOp

vars == <<live, wNode, wState, wTerm, hasRead, restore, fenceTerm, fenceOwner,
          rState, rWriter, inSync, delayOpen, acked, ackedBy, nextOp>>

wVars == <<wNode, wState, wTerm, hasRead, restore>>
ackVars == <<acked, ackedBy, nextOp>>

Ops == 1..MaxOps

MaxIssuedTerm == LET ts == {wTerm[w] : w \in Writers} \cup {fenceTerm} IN
                 CHOOSE t \in ts : \A u \in ts : u <= t

TypeOK ==
  /\ live \in [Nodes -> BOOLEAN]
  /\ wNode \in [Writers -> Nodes]
  /\ wState \in [Writers -> {"unborn", "serving", "restoring", "stopped"}]
  /\ wTerm \in [Writers -> Nat]
  /\ hasRead \in [Writers -> BOOLEAN]
  /\ restore \in [Writers -> SUBSET Ops]
  /\ fenceTerm \in Nat
  /\ fenceOwner \in Writers \cup {NoWriter}
  /\ rState \in {"assigned", "unassigned_existing", "initializing_remote"}
  /\ rWriter \in Writers \cup {NoWriter}
  /\ inSync \subseteq Writers
  /\ delayOpen \in BOOLEAN
  /\ acked \subseteq Ops
  /\ ackedBy \in [Writers -> SUBSET Ops]
  /\ nextOp \in 1..(MaxOps + 1)

(* w1 is the incumbent zero-replica primary, serving at term 1.            *)
Init ==
  /\ live = [n \in Nodes |-> TRUE]
  /\ \E w0 \in Writers, n0 \in Nodes :
       /\ wNode = [w \in Writers |-> n0]
       /\ wState = [w \in Writers |-> IF w = w0 THEN "serving" ELSE "unborn"]
       /\ wTerm = [w \in Writers |-> IF w = w0 THEN 1 ELSE 0]
       /\ rWriter = w0
       /\ fenceOwner = w0
       /\ inSync = {w0}
  /\ hasRead = [w \in Writers |-> FALSE]
  /\ restore = [w \in Writers |-> {}]
  /\ fenceTerm = 1
  /\ rState = "assigned"
  /\ delayOpen = FALSE
  /\ acked = {} /\ ackedBy = [w \in Writers |-> {}] /\ nextOp = 1

(***************************************************************************)
(* Acknowledging a write. The fence's whole contract, as proved by the     *)
(* sibling modules, is compressed into the owner gate: a serving writer    *)
(* acks only while the chain admits it, and a writer the chain refuses is  *)
(* fenced terminally. With FENCED = FALSE there is no gate - a partitioned *)
(* writer the cluster manager gave up on keeps acknowledging forever.      *)
(***************************************************************************)
AckWrite(w) ==
  /\ wState[w] = "serving"
  /\ nextOp <= MaxOps
  /\ IF FENCED /\ fenceOwner /= w
       THEN \* the chain refuses this writer: fenced is terminal
         /\ wState' = [wState EXCEPT ![w] = "stopped"]
         /\ UNCHANGED <<live, wNode, wTerm, hasRead, restore, fenceTerm,
                        fenceOwner, rState, rWriter, inSync, delayOpen, ackVars>>
       ELSE
         /\ acked' = acked \cup {nextOp}
         /\ ackedBy' = [ackedBy EXCEPT ![w] = @ \cup {nextOp}]
         /\ nextOp' = nextOp + 1
         /\ UNCHANGED <<live, wVars, fenceTerm, fenceOwner, rState, rWriter, inSync,
                        delayOpen>>

(***************************************************************************)
(* Node-left, as the cluster manager sees it. Two flavours with different  *)
(* writer fates:                                                           *)
(*  - a PARTITION removes the node from the view but the writer keeps      *)
(*    running and can still reach the object store (the dangerous case);   *)
(*  - a CRASH kills the writer too.                                        *)
(* Either way the routing entry becomes unassigned with an existing-store  *)
(* source and failedAllocations = 0 - exactly what                         *)
(* AllocationService#disassociateDeadNodes produces.                       *)
(***************************************************************************)
NodeLeftView(n) ==
  /\ live[n]
  /\ live' = [live EXCEPT ![n] = FALSE]
  /\ IF rState = "assigned" /\ rWriter /= NoWriter /\ wNode[rWriter] = n
       THEN /\ rState' = "unassigned_existing"
            /\ rWriter' = NoWriter
            \* delayed = (delayed_timeout > 0): both index configurations in
            \* one state space - TRUE is a live grace window, FALSE timeout 0.
            /\ \E d \in BOOLEAN : delayOpen' = d
       ELSE UNCHANGED <<rState, rWriter, delayOpen>>
  /\ UNCHANGED <<wVars, fenceTerm, fenceOwner, inSync, ackVars>>

NodeCrash(n) ==
  /\ live[n]
  /\ live' = [live EXCEPT ![n] = FALSE]
  /\ wState' = [w \in Writers |->
                  IF wNode[w] = n /\ wState[w] \in {"serving", "restoring"}
                  THEN "stopped" ELSE wState[w]]
  /\ IF rState = "assigned" /\ rWriter /= NoWriter /\ wNode[rWriter] = n
       THEN /\ rState' = "unassigned_existing"
            /\ rWriter' = NoWriter
            /\ \E d \in BOOLEAN : delayOpen' = d
       ELSE UNCHANGED <<rState, rWriter, delayOpen>>
  /\ UNCHANGED <<wNode, wTerm, hasRead, restore, fenceTerm, fenceOwner,
                 inSync, ackVars>>

NodeRejoinView(n) ==
  /\ ~live[n]
  /\ live' = [live EXCEPT ![n] = TRUE]
  /\ UNCHANGED <<wVars, fenceTerm, fenceOwner, rState, rWriter, inSync, delayOpen,
                 ackVars>>

(***************************************************************************)
(* THE TRIGGER. One atomic cluster-state update, which is faithful: the    *)
(* recovery-source re-point, the allocation to a live node, and the        *)
(* in-sync reset to the singleton new allocation id all happen inside one  *)
(* ClusterState computation (PrimaryShardAllocator decision +              *)
(* IndexMetadataUpdater), published as one new state. The new writer is    *)
(* appointed at a strictly higher term than any issued so far, which is    *)
(* what arms the fence takeover.                                           *)
(*                                                                          *)
(* Enabled exactly when the allocator has proven "no valid copy on any     *)
(* live node" - the NO_VALID_SHARD_COPY branch this feature converts.      *)
(* Nothing here checks whether the departed writer is truly dead; the      *)
(* fence is what makes that not matter.                                    *)
(*                                                                          *)
(* Eligibility, encoded in code rather than modeled: the trigger converts  *)
(* only OPEN, remote-backed, fenced indices whose shard carries an         *)
(* EXISTING_STORE recovery source - a shard that previously STARTED and    *)
(* owns a remote lineage. That is what excludes the concurrent-operation   *)
(* cases that would be wrong to auto-restore: a resize (shrink/split/      *)
(* clone) target mid-LOCAL_SHARDS-recovery has no remote lineage yet and   *)
(* restoring its empty remote store would lose the resize data; a          *)
(* snapshot-restore target carries a SNAPSHOT source and its own retry     *)
(* semantics; both also live under a different index UUID whose fence      *)
(* keyspace cannot contend with this shard's (the one-object-per-term,     *)
(* keyed-by-index-UUID invariant of RemoteStoreFence.java). The one shard  *)
(* modeled here is therefore an eligible shard; ineligible shards never    *)
(* reach the trigger.                                                       *)
(***************************************************************************)
(* The node-left grace window expiring: DelayedAllocationService schedules *)
(* a reroute at delayed_timeout expiry and AllocationService#              *)
(* removeDelayMarkers clears the marker on that reroute, re-running the    *)
(* allocation decision. Weakly fair below - the scheduler's promise.       *)
ExpireDelay ==
  /\ delayOpen
  /\ delayOpen' = FALSE
  /\ UNCHANGED <<live, wVars, fenceTerm, fenceOwner, rState, rWriter, inSync,
                 ackVars>>

TriggerAutoRestore(w, n) ==
  /\ rState = "unassigned_existing"
  \* The delay gate: while the node-left grace window is open the trigger
  \* declines, so a bouncing node can rejoin and reclaim its local copy
  \* (RejoinRecover below, which the window does NOT gate). Prevention is
  \* the primary's only protection - the rejoin cancellation machinery
  \* (ReplicaShardAllocator#processExistingRecoveries) covers replicas
  \* only, because historically an initializing primary WAS the only copy
  \* and there was nothing to cancel in favor of.
  /\ ~delayOpen
  /\ live[n]
  /\ wState[w] = "unborn"
  /\ \A v \in inSync : ~live[wNode[v]] \/ wState[v] = "stopped"  \* no valid live copy
  /\ MaxIssuedTerm + 1 <= MaxTerm
  /\ wState' = [wState EXCEPT ![w] = "restoring"]
  /\ wNode' = [wNode EXCEPT ![w] = n]
  /\ wTerm' = [wTerm EXCEPT ![w] = MaxIssuedTerm + 1]
  /\ rState' = "initializing_remote"
  /\ rWriter' = w
  /\ inSync' = {w}
  /\ UNCHANGED <<live, hasRead, restore, fenceTerm, fenceOwner, delayOpen,
                 ackVars>>

(***************************************************************************)
(* The operator's _remotestore/_restore, racing the trigger and the        *)
(* rejoin. It performs the SAME mutation but under a strictly WEAKER       *)
(* guard: the routing helper (initializeAsRemoteStoreRestore) requires     *)
(* only that the primary be unassigned - it does NOT require that no       *)
(* valid copy exists on a live node. So the operator can fire in the       *)
(* window where a rejoined in-sync copy is alive but not yet               *)
(* re-allocated, deliberately abandoning it - and can race RejoinRecover.  *)
(* Both restores and the rejoin are cluster-state updates, serialized by   *)
(* the cluster manager, which the interleaving of these atomic actions    *)
(* captures exactly: whichever runs first wins, the loser's guard is       *)
(* false. The abandoned live writer becomes stale and is refused by the    *)
(* fence on its next acknowledgement, then dropped as a stale copy.        *)
(***************************************************************************)
ManualRestore(w, n) ==
  /\ rState = "unassigned_existing"
  \* Deliberately NOT gated on ~delayOpen: the operator API ignores the
  \* node-left grace window too (a second guard-weakness beyond the
  \* no-valid-copy check), so it can abandon both the window and a
  \* rejoined-but-unallocated copy. TLC explores those interleavings.
  /\ live[n]
  /\ wState[w] = "unborn"
  /\ MaxIssuedTerm + 1 <= MaxTerm
  /\ wState' = [wState EXCEPT ![w] = "restoring"]
  /\ wNode' = [wNode EXCEPT ![w] = n]
  /\ wTerm' = [wTerm EXCEPT ![w] = MaxIssuedTerm + 1]
  /\ rState' = "initializing_remote"
  /\ rWriter' = w
  /\ inSync' = {w}
  /\ delayOpen' = FALSE
  /\ UNCHANGED <<live, hasRead, restore, fenceTerm, fenceOwner, ackVars>>

(***************************************************************************)
(* The restore target's recovery, decomposed to expose the races: seal     *)
(* first (strictly-greater-term takeover - the fence's deterministic       *)
(* victory), read the restore point strictly after the seal, then serve.   *)
(* The restore point read post-seal is 'acked' exactly: under FENCED the   *)
(* fence guarantees nothing more can be acked by superseded writers        *)
(* between seal and read (RemoteStoreFence.tla's sequenced-ack pipeline    *)
(* proves the metadata-visibility half), so the abstraction is faithful.   *)
(* Under FENCED = FALSE that guarantee is gone and the read races the old  *)
(* writer's acks - the trace TLC finds.                                    *)
(***************************************************************************)
Seal(w) ==
  /\ wState[w] = "restoring"
  /\ ~hasRead[w]
  /\ wTerm[w] > fenceTerm
  /\ fenceTerm' = wTerm[w]
  /\ fenceOwner' = w
  /\ UNCHANGED <<live, wNode, wState, wTerm, hasRead, restore, rState, rWriter,
                 inSync, delayOpen, ackVars>>

ReadRestorePoint(w) ==
  /\ wState[w] = "restoring"
  /\ fenceOwner = w                  \* reads its restore point after its own seal
  /\ ~hasRead[w]
  /\ hasRead' = [hasRead EXCEPT ![w] = TRUE]
  /\ restore' = [restore EXCEPT ![w] = acked]
  /\ UNCHANGED <<live, wNode, wState, wTerm, fenceTerm, fenceOwner, rState,
                 rWriter, inSync, delayOpen, ackVars>>

StartShard(w) ==
  /\ wState[w] = "restoring"
  /\ hasRead[w]
  /\ rState = "initializing_remote"
  /\ rWriter = w
  /\ wState' = [wState EXCEPT ![w] = "serving"]
  /\ rState' = "assigned"
  /\ UNCHANGED <<live, wNode, wTerm, hasRead, restore, fenceTerm, fenceOwner,
                 rWriter, inSync, delayOpen, ackVars>>

(***************************************************************************)
(* The restore target dies mid-restore (its node crashes). The routing     *)
(* entry goes back to unassigned and the trigger can fire again at a       *)
(* yet-higher term. The dead target's seal already advanced the fence,     *)
(* which is fine: the next target seals strictly above it.                 *)
(***************************************************************************)
RestoreTargetFails(w) ==
  /\ wState[w] = "restoring"
  /\ rState = "initializing_remote"
  /\ rWriter = w
  /\ wState' = [wState EXCEPT ![w] = "stopped"]
  /\ rState' = "unassigned_existing"
  /\ rWriter' = NoWriter
  \* No fresh grace window: in code the failed target keeps its REMOTE_STORE
  \* source and is re-placed by the balanced allocator, which nothing gates
  \* on the delay marker - only the EXISTING_STORE conversion is gated.
  /\ delayOpen' = FALSE
  /\ UNCHANGED <<live, wNode, wTerm, hasRead, restore, fenceTerm, fenceOwner,
                 inSync, ackVars>>

(***************************************************************************)
(* The departed node rejoins BEFORE the trigger fired: its copy is still   *)
(* the in-sync one, so the allocator re-allocates it (ordinary             *)
(* existing-store recovery of its own lineage). It re-reads its restore    *)
(* point; the fence admits it because nothing took the chain over. This    *)
(* is the path the trigger must NOT break - a flapping node is not a       *)
(* disaster, and re-allocation of the surviving copy stays legal.          *)
(***************************************************************************)
RejoinRecover(w) ==
  /\ rState = "unassigned_existing"
  /\ w \in inSync
  /\ live[wNode[w]]
  /\ wState[w] = "serving"           \* the partitioned writer, back in view
  /\ (FENCED => fenceOwner = w)      \* nothing took the chain over
  \* Not gated on the grace window: PrimaryShardAllocator ignores the delay
  \* marker, so the returning in-sync copy is re-allocated immediately -
  \* this is exactly what the window exists to make room for.
  /\ rState' = "assigned"
  /\ rWriter' = w
  /\ delayOpen' = FALSE
  /\ UNCHANGED <<live, wVars, fenceTerm, fenceOwner, inSync, ackVars>>

(***************************************************************************)
(* The departed node rejoins AFTER a restore superseded it: its allocation *)
(* id is no longer in-sync, so the applied cluster state drops its shard   *)
(* copy (IndicesClusterStateService#removeShards - "removing shard (stale  *)
(* copy)"). The writer stops without ever acknowledging past the fence.    *)
(***************************************************************************)
StaleCopyDropped(w) ==
  /\ live[wNode[w]]
  /\ wState[w] = "serving"
  /\ w \notin inSync
  /\ wState' = [wState EXCEPT ![w] = "stopped"]
  /\ UNCHANGED <<live, wNode, wTerm, hasRead, restore, fenceTerm, fenceOwner,
                 rState, rWriter, inSync, delayOpen, ackVars>>

Next ==
  \/ \E w \in Writers : AckWrite(w) \/ Seal(w) \/ ReadRestorePoint(w)
                        \/ StartShard(w) \/ RestoreTargetFails(w)
                        \/ RejoinRecover(w) \/ StaleCopyDropped(w)
  \/ \E n \in Nodes : NodeLeftView(n) \/ NodeCrash(n) \/ NodeRejoinView(n)
  \/ \E w \in Writers, n \in Nodes : TriggerAutoRestore(w, n) \/ ManualRestore(w, n)
  \/ ExpireDelay

(* The fair set is the allocator + restore pipeline: reroute runs on every *)
(* cluster-state change, so an enabled allocation decision - the trigger   *)
(* OR the re-allocation of a rejoined in-sync copy - is eventually taken,  *)
(* and a restoring target eventually progresses. Node flapping and writer  *)
(* failures stay unfair: the environment owes no promises.                 *)
RestoreActions == \E w \in Writers, n \in Nodes :
                    TriggerAutoRestore(w, n) \/ Seal(w) \/ ReadRestorePoint(w)
                    \/ StartShard(w) \/ RejoinRecover(w)

(* ExpireDelay is separately fair: DelayedAllocationService's scheduled    *)
(* reroute at delayed_timeout expiry is a system promise, not environment  *)
(* whim - without it an open window would block the trigger forever and    *)
(* RedEventuallyResolves would fail on the stalled-window behavior.        *)
Spec == Init /\ [][Next]_vars /\ WF_vars(RestoreActions) /\ WF_vars(ExpireDelay)

(***************************************************************************)
(* INVARIANTS                                                              *)
(***************************************************************************)

(* No acked write loss across the trigger: once the routing's serving      *)
(* writer has a restore point, every acknowledged operation is either in   *)
(* it or was acknowledged by that writer itself. This is the property the  *)
(* FENCED gate protects; ungated, the partitioned old primary acks after   *)
(* the target's restore-point read and TLC refutes it.                     *)
NoAckedWriteLoss ==
  (rState = "assigned" /\ rWriter /= NoWriter /\ hasRead[rWriter])
    => acked \subseteq (restore[rWriter] \cup ackedBy[rWriter])

(* One lineage: at most one writer is simultaneously in-sync and able to   *)
(* acknowledge. A resurrected stale copy never rejoins the lineage.        *)
SingleInSyncServing ==
  Cardinality({w \in Writers : w \in inSync /\ wState[w] \in {"serving", "restoring"}}) <= 1

(* The trigger never creates a second concurrent restore: the routing      *)
(* entry names at most one initializing target.                            *)
SingleRestoreTarget ==
  Cardinality({w \in Writers : wState[w] = "restoring"}) <= 1

(* Health, as ClusterShardHealth#getInactivePrimaryHealth derives it after *)
(* the RED-until-started decision: a hydrating primary cannot serve        *)
(* queries, so REMOTE_STORE is deliberately NOT in the YELLOW allow-list - *)
(* the restore window stays RED and converges to GREEN without operator    *)
(* action. The improvement over the parked state is convergence, not the  *)
(* color.                                                                  *)
Health ==
  IF rState = "assigned" THEN "GREEN" ELSE "RED"

(* The user-facing availability claim, made checkable: health never       *)
(* OVERSTATES availability. An in-flight restore reports RED (it cannot   *)
(* serve queries yet), and GREEN appears only when the routing has an     *)
(* assigned serving writer.                                                *)
HealthFaithful ==
  /\ (rState = "initializing_remote") => (Health = "RED")
  /\ (Health = "GREEN") => (rState = "assigned" /\ rWriter /= NoWriter)

(***************************************************************************)
(* LIVENESS: the shard does not rest in RED. Every entry into the          *)
(* unassigned (RED) state eventually resolves - the trigger fires and the  *)
(* shard leaves RED - unless the bounded model has genuinely run out of    *)
(* recovery material: no term headroom left (repeated restore-target       *)
(* failures consumed the bound), no unborn incarnation left, or no live    *)
(* node. Naming the exhaustion cases in the conclusion is what keeps the   *)
(* property honest under finite bounds: a behavior where every restore     *)
(* attempt is killed cannot be promised recovery by any system, and the    *)
(* disjunction confines the exception to exactly those behaviors. In       *)
(* every behavior with failure budget remaining, weak fairness of the      *)
(* restore pipeline forces the trigger and the shard leaves RED.           *)
(***************************************************************************)
RedEventuallyResolves ==
  (rState = "unassigned_existing")
    ~> (\/ rState /= "unassigned_existing"
        \/ MaxIssuedTerm >= MaxTerm
        \/ (\A w \in Writers : wState[w] /= "unborn")
        \/ (\A n \in Nodes : ~live[n]))

=====================================================================================
