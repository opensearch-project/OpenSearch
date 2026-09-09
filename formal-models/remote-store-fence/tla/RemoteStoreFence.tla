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
(* An acknowledgement is NOT one atomic step here. The implementation runs  *)
(* a metadata PUT and the fence CAS as separate store writes and joins both *)
(* before acking, so the model splits them too: StartUpload reserves the    *)
(* op, MetaPut makes its metadata visible to readers, CasAdvance advances   *)
(* the chain, JoinAck acknowledges once both landed. The SEQUENCED constant *)
(* selects the write-side ordering. FALSE lets the CAS land while the       *)
(* metadata PUT is still in flight - the original design, where the CAS ran *)
(* concurrently with the PUT to stay off the latency path. TLC refutes it:  *)
(* the CAS can win against the incumbent's object before a takeover's       *)
(* sweep, while the metadata lands only after the takeover read its restore *)
(* point - the acked op's metadata then sorts below every file the new      *)
(* owner publishes, and no later recovery resolves it. Acked-write loss,    *)
(* despite "seal before restore" holding: the upload straddles the sweep    *)
(* and the restore read, neither acked-before-seal nor refused-after. With  *)
(* SEQUENCED = TRUE the CAS is issued only after the metadata PUT           *)
(* completed, which restores the invariant by happens-before: CAS success   *)
(* means the incumbent's object still existed, so the sweep - and the       *)
(* restore read behind it - came later, and the metadata was already        *)
(* visible (read-after-write) when the restore point was read.              *)
(*                                                                          *)
(* Reading the restore point is correspondingly restore' = visible, not     *)
(* restore' = acked: a recovering copy resolves whatever metadata has       *)
(* landed, which can include a fenced writer's never-acknowledged orphan.   *)
(* Surfacing an unacknowledged operation is always permitted, so the        *)
(* superset is harmless - what NoAckedWriteLoss forbids is the converse.    *)
(*                                                                          *)
(* Deliberate abstractions:                                                 *)
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
  MaxOps,           \* bound on acknowledged operations (state-space bound only)
  SEQUENCED         \* TRUE: the fence CAS is issued only after the metadata PUT
                    \* completed (the shipped ordering). FALSE: CAS and PUT run
                    \* concurrently (the refuted original design).

ASSUME NoWriter \notin Writers
ASSUME SEQUENCED \in BOOLEAN

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
  restore,     \* the restore point (set of visible metadata) this writer read
  hasRead,     \* whether this writer has read its restore point
  (* Global acknowledgement history.                                      *)
  acked,       \* set of acknowledged operations
  ackedBy,     \* attribution: which writer acknowledged each op
  nextOp,
  (* The translog metadata files that have landed in the store - what a   *)
  (* recovering copy can resolve. Grows on MetaPut, never shrinks.        *)
  visible,
  (* Per-writer in-flight upload: the reserved op (0 = none) and which of *)
  (* its two store writes have landed. One upload at a time per writer,   *)
  (* which is the implementation's sync permit.                           *)
  upOp, upMetaDone, upCasDone

vars == <<fenceToken, fenceTerm, fenceSeq, fenceOwner,
          wTerm, wState, wHeld, wObserved, restore, hasRead,
          acked, ackedBy, nextOp, visible, upOp, upMetaDone, upCasDone>>

fenceVars  == <<fenceToken, fenceTerm, fenceSeq, fenceOwner>>
ackVars    == <<acked, ackedBy, nextOp>>
upVars     == <<visible, upOp, upMetaDone, upCasDone>>

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
  /\ visible \subseteq 1..MaxOps
  /\ upOp \in [Writers -> 0..MaxOps]
  /\ upMetaDone \in [Writers -> BOOLEAN]
  /\ upCasDone \in [Writers -> BOOLEAN]

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
  /\ visible = {}
  /\ upOp = [w \in Writers |-> 0]
  /\ upMetaDone = [w \in Writers |-> FALSE]
  /\ upCasDone = [w \in Writers |-> FALSE]

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
  /\ UNCHANGED <<fenceVars, wHeld, wObserved, restore, hasRead, ackVars, upVars>>

AppointRelocation(w) ==
  /\ wState[w] = "unborn"
  /\ \E v \in Writers :
       /\ wState[v] \in {"sealed", "active"}
       /\ wTerm' = [wTerm EXCEPT ![w] = wTerm[v]]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<fenceVars, wHeld, wObserved, restore, hasRead, ackVars, upVars>>

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
            /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, restore, hasRead, ackVars, upVars>>
       ELSE /\ wObserved' = [wObserved EXCEPT ![w] = fenceToken]
            /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, restore, hasRead, ackVars, upVars>>

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
         /\ UNCHANGED <<wTerm, restore, hasRead, ackVars, upVars>>
       ELSE \* Lost the race, so re-read and retry. Retrying above the fence
            \* term is always safe: "a higher term prevails".
         /\ wObserved' = [wObserved EXCEPT ![w] = -1]
         /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, restore, hasRead, ackVars, upVars>>

(***************************************************************************)
(* Reading the translog restore point, meaning the latest metadata a new    *)
(* copy recovers from. The shipped protocol does this strictly after the    *)
(* seal. What the store returns is the metadata that has LANDED - which     *)
(* can include a fenced writer's never-acknowledged orphan, a harmless      *)
(* superset - and cannot include metadata still in flight, which is the     *)
(* gap the SEQUENCED ordering exists to cover.                              *)
(***************************************************************************)
ReadRestorePoint(w) ==
  /\ wState[w] = "sealed"
  /\ restore' = [restore EXCEPT ![w] = visible]
  /\ hasRead' = [hasRead EXCEPT ![w] = TRUE]
  /\ wState' = [wState EXCEPT ![w] = "active"]
  /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, ackVars, upVars>>

(***************************************************************************)
(* Acknowledging a write - the upload pipeline. Two independent store       *)
(* writes back every acknowledgement: the immutable metadata PUT that makes *)
(* the operation resolvable, and the fence CAS that proves the writer still *)
(* owns the chain. The implementation joins both before acking; the model   *)
(* keeps them as separate steps so their interleaving with a takeover is    *)
(* explored rather than assumed away. SEQUENCED selects whether the CAS may *)
(* be issued while the metadata PUT is still in flight.                     *)
(***************************************************************************)
StartUpload(w) ==
  /\ wState[w] = "active"
  /\ upOp[w] = 0
  /\ nextOp <= MaxOps
  /\ upOp' = [upOp EXCEPT ![w] = nextOp]
  /\ nextOp' = nextOp + 1
  /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, wObserved, restore, hasRead,
                 acked, ackedBy, visible, upMetaDone, upCasDone>>

(* The metadata PUT lands. Deliberately enabled even for a writer that has  *)
(* been fenced in the meantime: an in-flight PUT completes regardless of    *)
(* what its issuer has since learned. That is how an orphan becomes         *)
(* visible.                                                                 *)
MetaPut(w) ==
  /\ upOp[w] /= 0
  /\ ~upMetaDone[w]
  /\ upMetaDone' = [upMetaDone EXCEPT ![w] = TRUE]
  /\ visible' = visible \cup {upOp[w]}
  /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, wObserved, restore, hasRead,
                 ackVars, upOp, upCasDone>>

(* The fence CAS. Under SEQUENCED it is issued only once the metadata PUT   *)
(* completed; unsequenced it can land first - and win against the           *)
(* incumbent's object before a takeover sweeps it, which is the refuted     *)
(* interleaving. A writer with a stale token finds out here and is fenced   *)
(* for good - "fenced is terminal" - so it never acknowledges the write.    *)
CasAdvance(w) ==
  /\ wState[w] = "active"
  /\ upOp[w] /= 0
  /\ ~upCasDone[w]
  /\ (SEQUENCED => upMetaDone[w])
  /\ IF wHeld[w] = fenceToken
       THEN /\ fenceToken' = fenceToken + 1
            /\ fenceTerm'  = wTerm[w]
            /\ fenceSeq'   = fenceSeq + 1
            /\ fenceOwner' = w
            /\ wHeld'      = [wHeld EXCEPT ![w] = fenceToken + 1]
            /\ upCasDone'  = [upCasDone EXCEPT ![w] = TRUE]
            /\ UNCHANGED <<wTerm, wState, wObserved, restore, hasRead, ackVars,
                           visible, upOp, upMetaDone>>
       ELSE /\ wState' = [wState EXCEPT ![w] = "fenced"]
            /\ UNCHANGED <<fenceVars, wTerm, wHeld, wObserved, restore, hasRead,
                           ackVars, upVars>>

(* Both store writes landed: the operation is acknowledged to the client.   *)
(* No store interaction remains, so nothing can refuse it - which is        *)
(* exactly why everything that must be true at ack time has to have been    *)
(* proven by the two writes above.                                          *)
JoinAck(w) ==
  /\ wState[w] = "active"
  /\ upOp[w] /= 0
  /\ upMetaDone[w]
  /\ upCasDone[w]
  /\ acked'      = acked \cup {upOp[w]}
  /\ ackedBy'    = [ackedBy EXCEPT ![w] = @ \cup {upOp[w]}]
  /\ upOp'       = [upOp EXCEPT ![w] = 0]
  /\ upMetaDone' = [upMetaDone EXCEPT ![w] = FALSE]
  /\ upCasDone'  = [upCasDone EXCEPT ![w] = FALSE]
  /\ UNCHANGED <<fenceVars, wTerm, wState, wHeld, wObserved, restore, hasRead,
                 nextOp, visible>>

Next ==
  \E w \in Writers :
    \/ AppointFailover(w)
    \/ AppointRelocation(w)
    \/ ReadFence(w)
    \/ TryClaim(w)
    \/ ReadRestorePoint(w)
    \/ StartUpload(w)
    \/ MetaPut(w)
    \/ CasAdvance(w)
    \/ JoinAck(w)

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
