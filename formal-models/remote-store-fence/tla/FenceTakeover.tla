------------------------------- MODULE FenceTakeover -------------------------------
(***************************************************************************)
(* Cross-term takeover, as implemented: one acknowledgement path per        *)
(* primary term.                                                            *)
(*                                                                          *)
(* The acknowledgement-path object is keyed by term, fence__<term>, and a   *)
(* takeover does four things:                                               *)
(*                                                                          *)
(*   1. Lists the paths, and refuses if a strictly higher term owns one.    *)
(*   2. Creates its own with create-if-absent. A lower-term incumbent never *)
(*      writes that key, so the create cannot be defeated.                  *)
(*   3. Deletes every lower-term object, unconditionally. This is the act   *)
(*      that fences the incumbent, whose next CAS then finds its object     *)
(*      gone.                                                               *)
(*   4. Lists again, and if a grant issued during the window superseded it, *)
(*      withdraws its own object and stands down.                           *)
(*                                                                          *)
(* A writer's only destructive act is deleting objects strictly BELOW its   *)
(* own term, so it can never touch a higher-term writer's path. Determinism *)
(* therefore comes from the key space rather than from winning a race,      *)
(* which is what NoHigherTermDefeat below states.                           *)
(*                                                                          *)
(* The cluster manager appears only through Appoint* actions. It issues     *)
(* monotonically increasing terms and performs no I/O. Its authority        *)
(* travels entirely in the term inside a KEY NAME - the only form in which  *)
(* an object store can order grants it cannot interpret.                    *)
(*                                                                          *)
(* NETWORK PARTITIONS are covered without a partition variable, because of  *)
(* how the model is built:                                                  *)
(*                                                                          *)
(*  - Cut off from the cluster: appointing a new copy does not stop an      *)
(*    existing one, and an existing copy may interleave acknowledgements at *)
(*    any point in any other copy's claim. A copy that keeps acting while   *)
(*    the cluster has moved on IS the partitioned writer, and every         *)
(*    interleaving of its actions is explored.                              *)
(*  - Cut off from the object store: such a copy simply takes no further    *)
(*    steps, which [][Next]_vars already allows, since TLA+ never requires  *)
(*    an enabled action to occur. A reachability flag would add nothing.    *)
(*  - Back-to-back promotions with claims in flight: appointment only       *)
(*    requires that a copy be unborn, so A may be mid-claim when B is       *)
(*    appointed and B mid-claim when C is appointed. TLC explores every     *)
(*    interleaving of their list, create, delete and verify steps.          *)
(*  - A lagging or deposed cluster manager: AppointStaleTerm starts a copy  *)
(*    at a term at or below one already issued, which the term floor has to *)
(*    refuse.                                                               *)
(*                                                                          *)
(* ASSUMED, not modeled: the object store is strongly consistent            *)
(* read-after-write, and its conditional write is atomic per key. Both hold *)
(* for S3, GCS and Azure. A store with stale reads would break the protocol *)
(* and is out of scope.                                                     *)
(*                                                                          *)
(* Deliberate abstractions. An acknowledgement is one atomic step - the     *)
(* implementation runs the metadata upload alongside the CAS and joins both *)
(* before acking, so a fenced writer can leave an orphan metadata file,     *)
(* which is a storage artifact and never an acknowledgement. Equal-term     *)
(* handoff ordering belongs to the relocation protocol and is modeled       *)
(* separately in FenceHandoff.tla.                                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Writers, MaxTerm, MaxOps

VARIABLES
  paths,        \* set of terms whose acknowledgement-path object exists
  pathToken,    \* term -> current version token of that object
  wTerm,        \* term the cluster manager appointed this copy at
  wState,       \* "unborn" | "fresh" | "claimed" | "sealed" | "active" | "fenced"
  wHeld,        \* token this copy believes is current for its own path
  wObserved,    \* token observed while arbitrating for an existing equal-term path
  wCreated,     \* whether this copy CREATED its own path (so whether it may withdraw it)
  wTook,        \* whether this copy has taken its path at all, by create or by arbitration
  wListed,      \* highest term seen by the pre-create listing (-1 = not listed)
  wFencedTerm,  \* the term that defeated this copy, for NoHigherTermDefeat
  restore, hasRead,
  acked, ackedBy, nextOp

vars == <<paths, pathToken, wTerm, wState, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
          restore, hasRead, acked, ackedBy, nextOp>>

(* The specification is symmetric in Writers - no action or invariant names a particular copy or
   orders them - so TLC may collapse permutations. That is sound here because only invariants
   are checked against this module. Symmetry is not sound with liveness properties in general. *)
Symm == Permutations(Writers)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Terms == 0..MaxTerm
HighestTerm == Max(paths \cup {0})
HighestIssuedTerm == Max({wTerm[v] : v \in Writers} \cup {HighestTerm})

Init ==
  /\ paths = {} /\ pathToken = [t \in Terms |-> 0]
  /\ wTerm = [w \in Writers |-> 0]
  /\ wState = [w \in Writers |-> "unborn"]
  /\ wHeld = [w \in Writers |-> 0]
  /\ wObserved = [w \in Writers |-> -1]
  /\ wCreated = [w \in Writers |-> FALSE]
  /\ wTook = [w \in Writers |-> FALSE]
  /\ wListed = [w \in Writers |-> -1]
  /\ wFencedTerm = [w \in Writers |-> -1]
  /\ restore = [w \in Writers |-> {}]
  /\ hasRead = [w \in Writers |-> FALSE]
  /\ acked = {} /\ ackedBy = [w \in Writers |-> {}]
  /\ nextOp = 1

(* Becoming fenced records the term that defeated us, so NoHigherTermDefeat can tell being
   "superseded" apart from being "out-raced". *)
Fenced(w, byTerm) ==
  /\ wState' = [wState EXCEPT ![w] = "fenced"]
  /\ wFencedTerm' = [wFencedTerm EXCEPT ![w] = byTerm]

(***************************************************************************)
(* Appointments. Failover issues a strictly higher term; a same-term        *)
(* appointment models a primary relocation target. Appointing a new copy    *)
(* does NOT stop the old one - that is the partitioned-writer case.         *)
(***************************************************************************)
AppointFailover(w) ==
  /\ wState[w] = "unborn"
  /\ HighestIssuedTerm + 1 <= MaxTerm
  /\ wTerm' = [wTerm EXCEPT ![w] = HighestIssuedTerm + 1]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<paths, pathToken, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
                 restore, hasRead, acked, ackedBy, nextOp>>

(* A lagging or deposed cluster manager - or a node acting on a stale cluster state - can start a
   copy at a term at or BELOW one already issued. Quorum stops such a cluster manager from
   committing state; it does not stop a node acting on state it already received. So the term
   floor has to refuse it. *)
AppointStaleTerm(w) ==
  /\ wState[w] = "unborn"
  /\ HighestIssuedTerm > 1
  /\ \E t \in 1..(HighestIssuedTerm - 1) : wTerm' = [wTerm EXCEPT ![w] = t]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<paths, pathToken, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
                 restore, hasRead, acked, ackedBy, nextOp>>

AppointRelocation(w) ==
  /\ wState[w] = "unborn"
  /\ \E v \in Writers :
       /\ wState[v] \in {"sealed", "active"}
       /\ wTerm' = [wTerm EXCEPT ![w] = wTerm[v]]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<paths, pathToken, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
                 restore, hasRead, acked, ackedBy, nextOp>>

(***************************************************************************)
(* Claim, step by step.                                                    *)
(***************************************************************************)
ListPaths(w) ==
  /\ wState[w] = "fresh"
  /\ wListed[w] = -1
  /\ IF \E t \in paths : t > wTerm[w]
       THEN /\ Fenced(w, HighestTerm)
            /\ UNCHANGED <<paths, pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed,
                           restore, hasRead, acked, ackedBy, nextOp>>
       ELSE /\ wListed' = [wListed EXCEPT ![w] = HighestTerm]
            /\ UNCHANGED <<paths, pathToken, wTerm, wState, wHeld, wObserved, wCreated, wTook,
                           wFencedTerm, restore, hasRead, acked, ackedBy, nextOp>>

CreatePath(w) ==
  /\ wState[w] = "fresh"
  /\ wListed[w] /= -1
  /\ wTook[w] = FALSE
  /\ wObserved[w] = -1
  /\ IF wTerm[w] \in paths
       THEN \* Equal-term twin: create-if-absent loses, so arbitrate by CAS. Observing the
            \* current token conveys no ownership.
         /\ wObserved' = [wObserved EXCEPT ![w] = pathToken[wTerm[w]]]
         /\ UNCHANGED <<paths, pathToken, wHeld, wCreated, wTook>>
       ELSE \* Create-if-absent on a key only this term's owner writes: uncontested.
         /\ paths' = paths \cup {wTerm[w]}
         /\ pathToken' = [pathToken EXCEPT ![wTerm[w]] = pathToken[wTerm[w]] + 1]
         /\ wHeld' = [wHeld EXCEPT ![w] = pathToken[wTerm[w]] + 1]
         /\ wCreated' = [wCreated EXCEPT ![w] = TRUE]
         /\ wTook' = [wTook EXCEPT ![w] = TRUE]
         /\ UNCHANGED wObserved
  /\ UNCHANGED <<wTerm, wState, wListed, wFencedTerm, restore, hasRead, acked, ackedBy, nextOp>>

(* Equal-term arbitration. The loser is defeated by an equal term, which counts as legitimate
   arbitration rather than a lower-term defeat. *)
ArbitrateSameTerm(w) ==
  /\ wState[w] = "fresh"
  /\ wObserved[w] /= -1
  /\ IF wTerm[w] \in paths /\ pathToken[wTerm[w]] = wObserved[w]
       THEN /\ pathToken' = [pathToken EXCEPT ![wTerm[w]] = pathToken[wTerm[w]] + 1]
            /\ wHeld' = [wHeld EXCEPT ![w] = pathToken[wTerm[w]] + 1]
            /\ wObserved' = [wObserved EXCEPT ![w] = -1]
            /\ wTook' = [wTook EXCEPT ![w] = TRUE]
            /\ UNCHANGED <<paths, wTerm, wState, wCreated, wListed, wFencedTerm,
                           restore, hasRead, acked, ackedBy, nextOp>>
       ELSE /\ Fenced(w, Max({wTerm[w]} \cup paths))
            /\ UNCHANGED <<paths, pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed,
                           restore, hasRead, acked, ackedBy, nextOp>>

(* Unconditional deletes: the act that fences a lower-term incumbent.
   The batch delete may only PARTIALLY succeed and still report success. S3's DeleteObjects
   reports per-key failures in its response body, and the shared blob-store helper logs them and
   returns normally, so an arbitrary subset of the lower-term paths may survive. VerifyClaim
   below therefore requires that none remain. That is what forces the implementation to check its
   sweep rather than trust it, and the sweep can be retried until it lands. *)
DeleteLowerPaths(w) ==
  /\ wState[w] = "fresh"
  /\ wObserved[w] = -1
  /\ wTerm[w] \in paths /\ wHeld[w] = pathToken[wTerm[w]]
  /\ \E t \in paths : t < wTerm[w]
  /\ \E survivors \in SUBSET {t \in paths : t < wTerm[w]} :
       paths' = {t \in paths : t >= wTerm[w]} \cup survivors
  /\ UNCHANGED <<pathToken, wTerm, wState, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
                 restore, hasRead, acked, ackedBy, nextOp>>

(* Re-list: a grant issued during our window supersedes us, so withdraw our own path. *)
VerifyClaim(w) ==
  /\ wState[w] = "fresh"
  /\ wObserved[w] = -1
  /\ wTerm[w] \in paths /\ wHeld[w] = pathToken[wTerm[w]]
  \* A surviving lower-term path means that copy can still acknowledge, so a claim may not be
  \* completed until the sweep has demonstrably landed.
  /\ ~(\E t \in paths : t < wTerm[w])
  /\ IF \E t \in paths : t > wTerm[w]
       THEN /\ paths' = IF wCreated[w] THEN paths \ {wTerm[w]} ELSE paths
            /\ Fenced(w, Max(paths \cup {0}))
            /\ UNCHANGED <<pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed,
                           restore, hasRead, acked, ackedBy, nextOp>>
       ELSE /\ wState' = [wState EXCEPT ![w] = "sealed"]
            /\ UNCHANGED <<paths, pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed,
                           wFencedTerm, restore, hasRead, acked, ackedBy, nextOp>>

(***************************************************************************)
(* Seal before restore, then acknowledge.                                  *)
(***************************************************************************)
ReadRestorePoint(w) ==
  /\ wState[w] = "sealed"
  /\ restore' = [restore EXCEPT ![w] = acked]
  /\ hasRead' = [hasRead EXCEPT ![w] = TRUE]
  /\ wState' = [wState EXCEPT ![w] = "active"]
  /\ UNCHANGED <<paths, pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed, wFencedTerm,
                 acked, ackedBy, nextOp>>

Ack(w) ==
  /\ wState[w] = "active"
  /\ nextOp <= MaxOps
  /\ IF wTerm[w] \in paths /\ wHeld[w] = pathToken[wTerm[w]]
       THEN /\ pathToken' = [pathToken EXCEPT ![wTerm[w]] = pathToken[wTerm[w]] + 1]
            /\ wHeld' = [wHeld EXCEPT ![w] = pathToken[wTerm[w]] + 1]
            /\ acked' = acked \cup {nextOp}
            /\ ackedBy' = [ackedBy EXCEPT ![w] = @ \cup {nextOp}]
            /\ nextOp' = nextOp + 1
            /\ UNCHANGED <<paths, wTerm, wState, wObserved, wCreated, wTook, wListed, wFencedTerm,
                           restore, hasRead>>
       ELSE /\ Fenced(w, Max(paths \cup {0}))
            /\ UNCHANGED <<paths, pathToken, wTerm, wHeld, wObserved, wCreated, wTook, wListed,
                           restore, hasRead, acked, ackedBy, nextOp>>

Next ==
  \E w \in Writers :
    \/ AppointFailover(w) \/ AppointRelocation(w) \/ AppointStaleTerm(w)
    \/ ListPaths(w) \/ CreatePath(w) \/ ArbitrateSameTerm(w)
    \/ DeleteLowerPaths(w) \/ VerifyClaim(w)
    \/ ReadRestorePoint(w) \/ Ack(w)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* SAFETY                                                                  *)
(***************************************************************************)

LiveOwners == {w \in Writers : wState[w] \in {"sealed", "active"}
                               /\ wTerm[w] \in paths
                               /\ wHeld[w] = pathToken[wTerm[w]]}

MutualExclusion == Cardinality(LiveOwners) <= 1

(* Nothing acknowledged may fall outside the restore point its successor serves from. *)
NoAckedWriteLoss ==
  \A w \in LiveOwners : hasRead[w] => acked \subseteq (restore[w] \cup ackedBy[w])

(* Determinism, stated as safety: a copy may only be defeated by a term at least as high as its
   own, and never out-raced by a LOWER-term incumbent. This is what the term-scoped key space
   gives us, and what a single shared acknowledgement-path object cannot provide. *)
NoHigherTermDefeat ==
  \A w \in Writers : wState[w] = "fenced" => wFencedTerm[w] >= wTerm[w]

AckAttribution ==
  /\ UNION {ackedBy[w] : w \in Writers} = acked
  /\ \A v, w \in Writers : v /= w => ackedBy[v] \cap ackedBy[w] = {}

=================================================================================
