---------------------------- MODULE RemoteStoreFence ----------------------------
(***************************************************************************)
(* Object-store-backed primary fencing for a remote-store shard.            *)
(*                                                                          *)
(* Models one shard's mutable `fence` blob, written only by                 *)
(* compare-and-swap (S3 If-Match / ETag), plus a set of shard-copy          *)
(* incarnations - "writers" - that can be alive at the same time. One of    *)
(* them may be a previous primary that is cut off from the cluster but can  *)
(* still reach the object store.                                            *)
(*                                                                          *)
(* The object store is the only synchronization point. Writers never talk   *)
(* to each other, or to the cluster manager; see the "no cluster-manager    *)
(* synchronization" invariant in RemoteStoreFence.java. The cluster manager *)
(* appears only through the monotonic terms that Appoint actions carry.     *)
(*                                                                          *)
(* The CAS itself is atomic, because the store serializes writes per key.   *)
(* But a claim is split into two steps here, ReadFence then TryClaim, so    *)
(* that races between a claimant and a still-acknowledging incumbent are    *)
(* part of the model rather than assumed away.                              *)
(*                                                                          *)
(* A takeover claims the chain BEFORE reading the translog restore point it *)
(* will serve from. That ordering is the whole point: read first and you    *)
(* leave a window where a cut-off previous primary still holds a valid      *)
(* token and can acknowledge writes landing after the restore point.        *)
(*                                                                          *)
(* The shipped protocol keys the acknowledgement-path object per primary    *)
(* term (fence__<term>), and a takeover creates its own key, then deletes   *)
(* every lower-term one. This module abstracts that as a SINGLE CAS         *)
(* register, which is faithful for the two questions it asks. Seal ordering *)
(* and acked-write loss are both properties of one term's chain and of the  *)
(* handover between terms, and one register captures both. The abstraction  *)
(* also leans on the recorded-ownership guard: a writer here retains its    *)
(* token from seal to ack, which is only faithful because the guarded       *)
(* re-adoption in the implementation refuses any chain a twin claimed in    *)
(* between - unguarded, the claim-twice window would fall outside this      *)
(* register, which is what FenceTakeover.tla checks. The term-scoped        *)
(* key space exists for a different property, takeover DETERMINISM, which   *)
(* FenceTakeover.tla studies and a single shared register cannot provide.   *)
(*                                                                          *)
(* Deliberate abstractions:                                                 *)
(*                                                                          *)
(*  - An acknowledgement is one atomic step: the CAS succeeds and the op is *)
(*    recorded. The implementation runs the metadata PUT alongside the CAS  *)
(*    and joins both before acking. The orphan metadata file a fenced       *)
(*    writer can leave is a storage artifact, not an acknowledgement, so it *)
(*    does not appear here.                                                 *)
(*                                                                          *)
(*  - A relocation target is modeled conservatively, as seal-then-read like *)
(*    any other takeover. The implementation is weaker: the target gets its *)
(*    state by handoff and takes the chain over only once the source hands  *)
(*    ownership to it. FenceHandoff.tla models that protocol, including the *)
(*    abort case it has to survive.                                         *)
(*                                                                          *)
(*  - The equal-term two-attempt give-up and the higher-term retry bound    *)
(*    are liveness policies. Safety does not depend on the schedule, so     *)
(*    bootstrap here just retries.                                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
  Writers,          \* shard-copy incarnations, e.g. {w1, w2, w3}
  NoWriter,         \* model value: "no owner"
  MaxTerm,          \* bound on primary terms (state-space bound only)
  MaxOps            \* bound on acknowledged operations (state-space bound only)

ASSUME NoWriter \notin Writers

VARIABLES
  (* The fence blob: one mutable object for the shard. token stands in     *)
  (* for the ETag. A write must present the current token and produces the *)
  (* next, so links cannot be skipped: holding the current token is what   *)
  (* it means to own the chain, and a writer that misses one can never     *)
  (* rejoin. CAS and the CAS chain are defined in full on RemoteStoreFence *)
  (* in the implementation.                                               *)
  fenceToken, fenceTerm, fenceSeq, fenceOwner,
  (* Per-writer state.                                                    *)
  wTerm,       \* term the copy was appointed at
  wState,      \* "unborn" | "fresh" | "sealed" | "active" | "fenced"
  wHeld,       \* token this writer believes is current (0 = none)
  wObserved,   \* token observed by an in-flight bootstrap read (-1 = none)
  restore,     \* the restore point (set of acked ops) this writer read
  hasRead,     \* whether this writer has read its restore point
  (* Global acknowledgement history.                                      *)
  acked,       \* set of acknowledged operations
  ackedBy,     \* attribution: which writer acknowledged each op
  nextOp

vars == <<fenceToken, fenceTerm, fenceSeq, fenceOwner,
          wTerm, wState, wHeld, wObserved, restore, hasRead,
          acked, ackedBy, nextOp>>

fenceVars  == <<fenceToken, fenceTerm, fenceSeq, fenceOwner>>
ackVars    == <<acked, ackedBy, nextOp>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

TypeOK ==
  /\ fenceToken \in Nat
  /\ fenceTerm \in Nat
  /\ fenceSeq \in Nat
  /\ fenceOwner \in Writers \cup {NoWriter}
  /\ wTerm \in [Writers -> Nat]
  /\ wState \in [Writers -> {"unborn", "fresh", "sealed", "active", "fenced"}]
  /\ wHeld \in [Writers -> Nat]
  /\ wObserved \in [Writers -> Nat \cup {-1}]
  /\ restore \in [Writers -> SUBSET (1..MaxOps)]
  /\ hasRead \in [Writers -> BOOLEAN]
  /\ acked \subseteq 1..MaxOps
  /\ ackedBy \in [Writers -> SUBSET (1..MaxOps)]
  /\ nextOp \in 1..(MaxOps + 1)

Init ==
  /\ fenceToken = 0 /\ fenceTerm = 0 /\ fenceSeq = 0 /\ fenceOwner = NoWriter
  /\ wTerm = [w \in Writers |-> 0]
  /\ wState = [w \in Writers |-> "unborn"]
  /\ wHeld = [w \in Writers |-> 0]
  /\ wObserved = [w \in Writers |-> -1]
  /\ restore = [w \in Writers |-> {}]
  /\ hasRead = [w \in Writers |-> FALSE]
  /\ acked = {} /\ ackedBy = [w \in Writers |-> {}]
  /\ nextOp = 1

(***************************************************************************)
(* Appointments. This is the cluster manager's only contribution. Terms     *)
(* are issued monotonically, so a failover appoints strictly above every    *)
(* term issued so far, and a same-term appointment stands for a             *)
(* relocation target. Note that appointing a new writer does NOT stop the   *)
(* old one. That is the partitioned-writer case.                            *)
(***************************************************************************)
HighestIssuedTerm == Max({wTerm[v] : v \in Writers} \cup {fenceTerm})

AppointFailover(w) ==
  /\ wState[w] = "unborn"
  /\ HighestIssuedTerm + 1 <= MaxTerm
  /\ wTerm' = [wTerm EXCEPT ![w] = HighestIssuedTerm + 1]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<fenceVars, wHeld, wObserved, restore, hasRead, ackVars>>

AppointRelocation(w) ==
  /\ wState[w] = "unborn"
  /\ \E v \in Writers :
       /\ wState[v] \in {"sealed", "active"}
       /\ wTerm' = [wTerm EXCEPT ![w] = wTerm[v]]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<fenceVars, wHeld, wObserved, restore, hasRead, ackVars>>

(***************************************************************************)
(* Bootstrap, or seal: read the blob, then CAS on the token just read.      *)
(* The read also applies the term floor - a fence already above the         *)
(* claimant's term refuses the claim outright, which is what "the term      *)
(* never regresses" means.                                                  *)
(***************************************************************************)
ReadFence(w) ==
  /\ wState[w] = "fresh"
  /\ wObserved[w] = -1
  /\ IF fenceTerm > wTerm[w]
       THEN /\ wState' = [wState EXCEPT ![w] = "fenced"]
            /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, restore, hasRead, ackVars>>
       ELSE /\ wObserved' = [wObserved EXCEPT ![w] = fenceToken]
            /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, restore, hasRead, ackVars>>

TryClaim(w) ==
  /\ wState[w] = "fresh"
  /\ wObserved[w] /= -1
  /\ IF fenceToken = wObserved[w]
       THEN \* the CAS lands: this writer owns the chain now
         /\ fenceToken' = fenceToken + 1
         /\ fenceTerm'  = wTerm[w]
         /\ fenceSeq'   = fenceSeq + 1
         /\ fenceOwner' = w
         /\ wHeld'      = [wHeld EXCEPT ![w] = fenceToken + 1]
         /\ wObserved'  = [wObserved EXCEPT ![w] = -1]
         /\ wState'     = [wState EXCEPT ![w] = "sealed"]
         /\ UNCHANGED <<wTerm, restore, hasRead, ackVars>>
       ELSE \* Lost the race, so re-read and retry. Retrying above the fence
            \* term is always safe: "a higher term prevails".
         /\ wObserved' = [wObserved EXCEPT ![w] = -1]
         /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, restore, hasRead, ackVars>>

(***************************************************************************)
(* Reading the translog restore point, meaning the latest metadata a new    *)
(* copy recovers from. The shipped protocol does this strictly after the    *)
(* seal. The buggy variant does it first.                                   *)
(***************************************************************************)
ReadRestorePoint(w) ==
  /\ wState[w] = "sealed"
  /\ restore' = [restore EXCEPT ![w] = acked]
  /\ hasRead' = [hasRead EXCEPT ![w] = TRUE]
  /\ wState' = [wState EXCEPT ![w] = "active"]
  /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, ackVars>>

(***************************************************************************)
(* Acknowledging a write: one atomic CAS on the chain. A writer with a      *)
(* stale token finds out here and is fenced for good - "fenced is           *)
(* terminal" - so it never acknowledges the write.                          *)
(***************************************************************************)
Ack(w) ==
  /\ wState[w] = "active"
  /\ nextOp <= MaxOps
  /\ IF wHeld[w] = fenceToken
       THEN /\ fenceToken' = fenceToken + 1
            /\ fenceTerm'  = wTerm[w]
            /\ fenceSeq'   = fenceSeq + 1
            /\ fenceOwner' = w
            /\ wHeld'      = [wHeld EXCEPT ![w] = fenceToken + 1]
            /\ acked'      = acked \cup {nextOp}
            /\ ackedBy'    = [ackedBy EXCEPT ![w] = @ \cup {nextOp}]
            /\ nextOp'     = nextOp + 1
            /\ UNCHANGED <<wTerm, wState, wObserved, restore, hasRead>>
       ELSE /\ wState' = [wState EXCEPT ![w] = "fenced"]
            /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, restore, hasRead, ackVars>>

Next ==
  \E w \in Writers :
    \/ AppointFailover(w)
    \/ AppointRelocation(w)
    \/ ReadFence(w)
    \/ TryClaim(w)
    \/ ReadRestorePoint(w)
    \/ Ack(w)

Spec == Init /\ [][Next]_vars
             /\ \A w \in Writers : WF_vars(ReadFence(w)) /\ WF_vars(TryClaim(w))

(***************************************************************************)
(* INVARIANTS (safety - checked exhaustively by TLC)                       *)
(***************************************************************************)

(* One writer at a time: at most one live writer holds the current          *)
(* token, and that writer is the recorded owner. Formally:                  *)
(*   |{w : wState[w] IN {sealed,active} AND wHeld[w] = fenceToken}| <= 1    *)
MutualExclusion ==
  LET holders == {w \in Writers : wState[w] \in {"sealed", "active"} /\ wHeld[w] = fenceToken}
  IN /\ Cardinality(holders) <= 1
     /\ \A w \in holders : fenceOwner = w

(* No acked write loss. This is the one that matters most. Once the         *)
(* chain's owner has read its restore point, every acknowledged operation   *)
(* is either in that restore point or was acknowledged by the owner         *)
(* itself:                                                                  *)
(*   owner /= None AND hasRead[owner] =>                                    *)
(*     acked SUBSETEQ restore[owner] UNION ackedBy[owner]                   *)
(* Put another way: nothing a superseded writer acknowledged can land       *)
(* after the restore point its successor serves from.                       *)
NoAckedWriteLoss ==
  (fenceOwner /= NoWriter /\ hasRead[fenceOwner])
    => acked \subseteq (restore[fenceOwner] \cup ackedBy[fenceOwner])

(* Every acknowledged op was acknowledged by exactly one writer.          *)
AckAttribution ==
  /\ UNION {ackedBy[w] : w \in Writers} = acked
  /\ \A v, w \in Writers : v /= w => ackedBy[v] \cap ackedBy[w] = {}

(* The fence term never exceeds a term the cluster issued, and the          *)
(* owner's term is the fence term.                                          *)
OwnerTermAgreement ==
  fenceOwner /= NoWriter => fenceTerm = wTerm[fenceOwner]

(***************************************************************************)
(* ACTION PROPERTIES (checked as temporal properties)                      *)
(***************************************************************************)

(* The term never regresses: [][fenceTerm' >= fenceTerm]_vars             *)
TermMonotonic == [][fenceTerm' >= fenceTerm]_vars

(* Seq strictly increases along the chain, in lockstep with the token:    *)
(*   [][fenceToken' > fenceToken => fenceSeq' = fenceSeq + 1]_vars        *)
SeqMonotonic == [][fenceToken' > fenceToken => fenceSeq' = fenceSeq + 1]_vars

(* Fenced is terminal: [][forall w : fenced => fenced']_vars              *)
FencedTerminal == [][\A w \in Writers : wState[w] = "fenced" => wState'[w] = "fenced"]_vars

(***************************************************************************)
(* LIVENESS: a higher term prevails. A fresh claimant above the fence       *)
(* term eventually leads to the chain reaching at least that term -         *)
(* either this claimant wins, or an even higher one does. It holds under    *)
(* weak fairness of the claimant's read and CAS, because acknowledgements   *)
(* are finite in the model. The implementation's counterpart is the         *)
(* bounded-retry policy, whose exhaustion is retryable, not fatal.          *)
(***************************************************************************)
HigherTermPrevails ==
  \A w \in Writers :
    (wState[w] = "fresh" /\ wTerm[w] > fenceTerm) ~> (fenceTerm >= wTerm[w] \/ wState[w] = "fenced")

=================================================================================
