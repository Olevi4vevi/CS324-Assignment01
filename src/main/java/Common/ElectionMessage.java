package Common;

import java.io.Serializable;
import java.util.UUID;

/**
 * Flooded through the unstructured network during a leader election.
 *
 * Carries the best candidate (lowest JAC, tie-break highest ID) known so far
 * as this message has travelled. electionId identifies one election round -
 * it is the key each worker uses to recognise "I've already dealt with this
 * election" so it doesn't reprocess or endlessly re-flood the same
 * information (see WorkerImpl.receiveElection).
 *
 * There is deliberately no "visited" list gating who the message may still
 * reach: a worker re-floods to ALL of its neighbors whenever the message
 * teaches it something new (a strictly better candidate than it already
 * knew), including neighbors that have already seen an earlier, weaker
 * version of this election. That is what lets a better candidate discovered
 * deep in the network flow back to every other worker, not just onward away
 * from the initiator. Termination is guaranteed because a worker only
 * re-forwards on genuine improvement, and improvement cannot happen forever
 * in a finite network with a fixed comparison rule.
 */
public class ElectionMessage implements Serializable {
    private final UUID electionId;
    private final int candidateId;
    private final int candidateJac;

    public ElectionMessage(UUID electionId, int candidateId, int candidateJac) {
        this.electionId = electionId;
        this.candidateId = candidateId;
        this.candidateJac = candidateJac;
    }

    public UUID getElectionId() {
        return electionId;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getCandidateJac() {
        return candidateJac;
    }
}
