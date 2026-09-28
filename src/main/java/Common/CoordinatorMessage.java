package Common;

import java.io.Serializable;
import java.util.UUID;

/**
 * Flooded through the network once an election concludes, so every reachable
 * worker learns who the new coordinator is. Carries the same electionId as
 * the ElectionMessage it resulted from: a worker commits to the FIRST
 * CoordinatorMessage it sees for a given electionId and ignores/does not
 * re-forward any later one for that same round, even if (due to the
 * finalize-on-timeout design) more than one worker ends up broadcasting a
 * result for it. This is what keeps a single election round from being
 * "processed" more than once per worker.
 */
public class CoordinatorMessage implements Serializable {
    private final UUID electionId;
    private final int coordinatorId;

    public CoordinatorMessage(UUID electionId, int coordinatorId) {
        this.electionId = electionId;
        this.coordinatorId = coordinatorId;
    }

    public UUID getElectionId() {
        return electionId;
    }

    public int getCoordinatorId() {
        return coordinatorId;
    }
}
