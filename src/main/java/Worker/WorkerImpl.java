package Worker;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import Common.BootstrapRMI;
import Common.Constants;
import Common.CoordinatorMessage;
import Common.ElectionMessage;
import Common.Job;
import Common.JobResult;
import Common.JobType;
import Common.WorkerRMI;

/**
 * RMI plumbing for a worker node: ID assignment, neighbor list management,
 * the JAC counter, leader election, coordinator-result propagation, and
 * (as coordinator) splitting an incoming Job into equal chunks across every
 * active worker, plus polling those workers' getJobResult() until every
 * chunk is back and combining them into the final answer. Every worker also
 * computes whatever chunk(s) it is assigned (MAX / PRIMESUM / PRIMECOUNT) on
 * its own job thread pool, so it can work on several chunks concurrently.
 */
public class WorkerImpl extends UnicastRemoteObject implements WorkerRMI {

    // Required by the assignment spec.
    public static final String leaderman = "cs324";

    // Generous on purpose: when starting workers by hand in separate
    // terminals, the gap between "type the command" and "the JVM actually
    // registers and connects" can easily exceed a couple of seconds
    // (cold JVM start, RMI handshake, human typing speed). A short timeout
    // lets the first worker conclude its own trivial (self-only) election
    // before later workers have even connected, so they never receive that
    // round's COORDINATOR broadcast. 12s gives everyone time to join in.
    private static final long ELECTION_TIMEOUT_MS = 6000;

    private volatile int id = -1;
    private final AtomicInteger jac = new AtomicInteger(0);
    // Neighbors are tracked by ID only (per the professor's guidance) - the
    // actual stub is looked up in the shared RMI registry each time a
    // message needs to be sent, rather than cached long-term.
    private final Set<Integer> neighborIds = ConcurrentHashMap.newKeySet();
    private volatile Registry registry;
    private volatile int coordinatorID = -1;
    private volatile boolean isCoordinator = false;
    private final AtomicInteger jobsAssignedThisTerm = new AtomicInteger(0);

    // Election bookkeeping, keyed by electionId so every round is independent.
    // electionBest holds, per round, the best (lowest JAC, tie -> highest ID)
    // candidate this worker has seen so far - this IS the dedup/"don't
    // process the same message twice" mechanism: an incoming ElectionMessage
    // is only acted on (and re-flooded) when it improves this value.
    private final Map<UUID, AtomicReference<Candidate>> electionBest = new ConcurrentHashMap<>();
    // Once a worker commits to a coordinator for a given electionId, any
    // further CoordinatorMessage for that same electionId is ignored - this
    // is the dedup mechanism for the coordinator-announcement phase.
    private final Map<UUID, Integer> decidedCoordinatorByElection = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService messageExecutor = Executors.newCachedThreadPool();

    // Reference used only by whichever worker is currently coordinator, to
    // ask Bootstrap which workers are active when splitting a job into chunks.
    private volatile BootstrapRMI bootstrap;

    // Job execution is kept on its own executors, separate from the election
    // machinery above, so a slow job never delays election/coordinator
    // messages and vice versa. jobExecutor lets this worker compute several
    // assigned chunks concurrently (a worker "juggling multiple jobs" is just
    // several tasks in flight in this pool at once); jobPollScheduler lets a
    // coordinator poll several in-flight jobs' chunk results concurrently
    // without one job's polling loop blocking another's.
    private final ExecutorService jobExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService jobPollScheduler = Executors.newScheduledThreadPool(4);

    // Chunk-level results this worker has personally computed, keyed by the
    // chunk's own jobID (e.g. "<originalJobID>-chunk0").
    private final Map<String, JobResult> localChunkResults = new ConcurrentHashMap<>();
    // Combined/final results for jobs this worker split as coordinator, keyed
    // by the ORIGINAL jobID the client submitted (not any chunk ID).
    private final Map<String, JobResult> finalJobResults = new ConcurrentHashMap<>();

    // Marks a Job's ID as a chunk assigned directly to one worker, as opposed
    // to a fresh top-level job a client just submitted to the coordinator.
    // Safe as a plain substring check: client-submitted job IDs are random
    // UUIDs (hex + hyphens only), which can never contain "chunk".
    private static final String CHUNK_MARKER = "-chunk";
    private static final long JOB_POLL_INTERVAL_MS = 500;

    // How long the coordinator waits for a chunk's result before treating
    // that worker as unresponsive, and how many times it will reassign the
    // same chunk to a different worker before giving up on the whole job.
    private static final long CHUNK_TIMEOUT_MS = 15000;
    private static final int MAX_CHUNK_RETRIES = 2; // i.e. up to 3 attempts total per chunk

    public WorkerImpl() throws RemoteException {
        super();
    }

    /** Set once, right after Bootstrap.registerWorker(...) returns the assigned ID. */
    public void setAssignedID(int assignedID) {
        this.id = assignedID;
    }

    /**
     * Set once at startup so this worker can resolve a neighbor's current
     * stub from its ID on demand (registry.lookup(Constants.WORKER_NAME_PREFIX + id)).
     */
    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * Set once at startup, right after WorkerNode registers with Bootstrap.
     * Only ever used by whichever worker currently holds the coordinator
     * role, to find out which workers are active when splitting a job.
     */
    public void setBootstrap(BootstrapRMI bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public int getID() throws RemoteException {
        return id;
    }

    @Override
    public int getJAC() throws RemoteException {
        return jac.get();
    }

    /** Local helper for whoever implements coordinator job-assignment logic. */
    public int incrementAndGetJAC() {
        return jac.incrementAndGet();
    }

    @Override
    public void addNeighborWorker(int neighborID) throws RemoteException {
        neighborIds.add(neighborID);
        System.out.println("[Worker " + id + "] Connected to neighbor " + neighborID);
    }

    @Override
    public List<Integer> getNeighborIDs() throws RemoteException {
        return new ArrayList<>(neighborIds);
    }

    /**
     * Resolves a neighbor's current stub by ID via the shared RMI registry.
     * Returns null (and logs) if the lookup fails - e.g. the neighbor has
     * crashed - so callers can just skip that neighbor for this message.
     */
    private WorkerRMI resolveStub(int workerId) {
        try {
            return (WorkerRMI) registry.lookup(Constants.WORKER_NAME_PREFIX + workerId);
        } catch (NotBoundException | RemoteException e) {
            System.out.println("[Worker " + id + "] Could not resolve worker " + workerId + " via registry: " + e.getMessage());
            return null;
        }
    }

    @Override 
    public int getCoordinatorID() {
        return coordinatorID;
    }

    public boolean isCoordinator() {
        return isCoordinator;
    }

    // ------------------------------------------------------------------
    // Leader election
    // ------------------------------------------------------------------

    /**
     * Immutable snapshot of "who's the best candidate we've seen in this
     * election round" - lowest JAC wins, ties broken by highest ID (per spec).
     */
    private static final class Candidate {
        final int id;
        final int jac;

        Candidate(int id, int jac) {
            this.id = id;
            this.jac = jac;
        }

        boolean isBetterThan(Candidate other) {
            if (this.jac != other.jac) {
                return this.jac < other.jac;
            }
            return this.id > other.id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Candidate)) return false;
            Candidate c = (Candidate) o;
            return id == c.id && jac == c.jac;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, jac);
        }
    }

    /**
     * Kicks off a new election round. Any worker may call this when it
     * believes there is no active coordinator (e.g. it is the first node in
     * the network, or the current term just ended).
     */
    public void startElection() {
        UUID electionId = UUID.randomUUID();
        System.out.println("[Worker " + id + "] Starting election " + electionId);

        electionBest.put(electionId, new AtomicReference<>(new Candidate(id, jac.get())));
        scheduleElectionTimeout(electionId);

        ElectionMessage msg = new ElectionMessage(electionId, id, jac.get());
        propagateElection(msg);
    }

    @Override
    public void receiveElection(ElectionMessage msg) throws RemoteException {
        UUID electionId = msg.getElectionId();
        Candidate incoming = new Candidate(msg.getCandidateId(), msg.getCandidateJac());

        boolean[] firstTime = {false};
        AtomicReference<Candidate> ref = electionBest.computeIfAbsent(electionId, k -> {
            // First time we've heard of this election round at all: start our
            // own timeout too, in case this worker ends up having to declare
            // the result itself (e.g. if the original initiator crashes).
            firstTime[0] = true;
            scheduleElectionTimeout(electionId);
            return new AtomicReference<>(new Candidate(id, jac.get()));
        });

        Candidate before = ref.getAndUpdate(current -> current.isBetterThan(incoming) ? current : incoming);
        Candidate after = ref.get();
        boolean improved = !after.equals(before);

        // Forward whenever this is the first time we've heard of the
        // election (so the news that an election is happening reaches every
        // corner of the network at least once, even if our own candidate
        // already happens to beat the one in this particular message), OR
        // whenever the message actually improved our recorded best candidate
        // (so a better candidate discovered further out can flow back to
        // parts of the network that only knew about a weaker one). Once a
        // worker's recorded candidate stops changing, it stops forwarding,
        // so the flood naturally dies out instead of looping forever - that
        // is what stops the same election information being reprocessed
        // (and re-broadcast) more than once by any given worker.
        if (firstTime[0] || improved) {
            ElectionMessage forward = new ElectionMessage(electionId, after.id, after.jac);
            propagateElection(forward);
        }
    }

    private void propagateElection(ElectionMessage msg) {
        for (int neighborId : neighborIds) {
            messageExecutor.submit(() -> {
                WorkerRMI neighborStub = resolveStub(neighborId);
                if (neighborStub == null) {
                    return;
                }
                try {
                    neighborStub.receiveElection(msg);
                } catch (RemoteException e) {
                    System.out.println("[Worker " + id + "] Neighbor " + neighborId + " unreachable during election.");
                }
            });
        }
    }

    private void scheduleElectionTimeout(UUID electionId) {
        scheduler.schedule(() -> finalizeElection(electionId), ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * After giving the flood enough time to reach and converge across every
     * worker reachable from here, announce whatever candidate we ended up
     * with. Every worker that saw this election independently does the same
     * thing; since the comparison rule is deterministic and the flood has
     * had time to fully converge, they all arrive at the same winner, so
     * redundant COORDINATOR broadcasts are harmless (the first one any
     * worker receives for this electionId is the one it commits to; later
     * ones for the same electionId are ignored - see receiveCoordinatorMsg).
     */
    private void finalizeElection(UUID electionId) {
        AtomicReference<Candidate> ref = electionBest.get(electionId);
        if (ref == null) {
            return;
        }
        Candidate winner = ref.get();
        System.out.println("[Worker " + id + "] Election " + electionId + " concluded -> coordinator " + winner.id);

        CoordinatorMessage msg = new CoordinatorMessage(electionId, winner.id);
        boolean applied = applyCoordinatorResult(msg);
        if (applied) {
            propagateCoordinator(msg);
        }
    }

    @Override
    public void receiveCoordinatorMsg(CoordinatorMessage msg) throws RemoteException {
        boolean applied = applyCoordinatorResult(msg);
        // Same duplicate-prevention idea as elections: only re-forward the
        // first time we commit to a result for this particular electionId.
        if (applied) {
            propagateCoordinator(msg);
        }
    }

    private void propagateCoordinator(CoordinatorMessage msg) {
        for (int neighborId : neighborIds) {
            messageExecutor.submit(() -> {
                WorkerRMI neighborStub = resolveStub(neighborId);
                if (neighborStub == null) {
                    return;
                }
                try {
                    neighborStub.receiveCoordinatorMsg(msg);
                } catch (RemoteException e) {
                    System.out.println("[Worker " + id + "] Neighbor " + neighborId + " unreachable for coordinator broadcast.");
                }
            });
        }
    }

    /**
     * Commits to a coordinator result for msg's electionId, but only the
     * first time this worker sees a result for that round - a later
     * CoordinatorMessage for the same electionId (even a different value)
     * is ignored so an already-decided result can't be overwritten. Returns
     * true iff this call is the one that actually applied the result (i.e.
     * the caller should go on to (re)propagate it).
     */
    private synchronized boolean applyCoordinatorResult(CoordinatorMessage msg) {
        Integer existing = decidedCoordinatorByElection.putIfAbsent(msg.getElectionId(), msg.getCoordinatorId());
        if (existing != null) {
            return false; // already decided this round - ignore
        }

        this.coordinatorID = msg.getCoordinatorId();
        this.isCoordinator = (msg.getCoordinatorId() == this.id);
        jobsAssignedThisTerm.set(0);
        if (this.isCoordinator) {
            System.out.println("[Worker " + id + "] I am the new coordinator for this term.");
        } else {
            System.out.println("[Worker " + id + "] Learned new coordinator: " + this.coordinatorID);
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Term / JAC integration point for whoever owns job distribution.
    // ------------------------------------------------------------------

    /**
     * Call this exactly once per job the coordinator dispatches into the
     * network (not once per worker that a single job's workload is split
     * across). Only valid to call on the worker instance that is currently
     * the coordinator.
     *
     * Increments this coordinator's JAC and, once
     * Constants.MAX_JOBS_PER_ELECTION jobs have been assigned this term,
     * automatically starts a new election (the term limit from the spec).
     */
    public synchronized void onJobAssigned() {
        if (!isCoordinator) {
            throw new IllegalStateException("Worker " + id + " is not the coordinator - cannot assign jobs.");
        }
        jac.incrementAndGet();
        int assigned = jobsAssignedThisTerm.incrementAndGet();
        System.out.println("[Worker " + id + "] JAC now " + jac.get()
                + " (" + assigned + "/" + Constants.MAX_JOBS_PER_ELECTION + " jobs this term)");

        if (assigned >= Constants.MAX_JOBS_PER_ELECTION) {
            System.out.println("[Worker " + id + "] Term limit reached - starting new election.");
            isCoordinator = false;
            coordinatorID = -1;
            startElection();
        }
    }

    // ------------------------------------------------------------------
    // Job processing: MAX / PRIMESUM / PRIMECOUNT
    // ------------------------------------------------------------------

    /**
     * Entry point for both roles a Job can arrive as:
     *  - A fresh top-level job a client submitted to whoever is coordinator
     *    right now (plain UUID jobID) -> gets split into chunks and dispatched.
     *  - A single chunk the coordinator assigned directly to this worker
     *    (jobID contains CHUNK_MARKER) -> gets computed locally.
     */
    @Override
    public void receiveJob(Job job) throws RemoteException {
        if (isChunkJob(job.getJobID())) {
            executeChunkLocally(job);
        } else {
            splitAndDispatch(job);
        }
    }

    @Override
    public JobResult getJobResult(String jobID) throws RemoteException {
        // A caller could be asking about either an original top-level jobID
        // (if this worker was/is the coordinator for it) or a chunk jobID
        // (if this worker was assigned that chunk to compute) - check both.
        JobResult combined = finalJobResults.get(jobID);
        if (combined != null) {
            return combined;
        }
        return localChunkResults.get(jobID);
    }

    private boolean isChunkJob(String jobID) {
        return jobID.contains(CHUNK_MARKER);
    }

    /** Runs one chunk's computation in the job thread pool and records its result when done. */
    private void executeChunkLocally(Job chunk) {
        jobExecutor.submit(() -> {
            int result = compute(chunk);
            localChunkResults.put(chunk.getJobID(), new JobResult(chunk.getJobID(), result, id));
            System.out.println("[Worker " + id + "] Completed chunk " + chunk.getJobID() + " -> " + result);
        });
    }

    private int compute(Job job) {
        switch (job.getType()) {
            case MAX -> {
                return job.getNumbers().stream()
                        .mapToInt(Integer::intValue)
                        .max()
                        .orElse(Integer.MIN_VALUE); // identity element for MAX, harmless if combined with real values
            }
            case PRIMESUM -> {
                int sum = 0;
                for (int n = job.getRangeStart(); n <= job.getRangeEnd(); n++) {
                    if (isPrime(n)) {
                        sum += n;
                    }
                }
                return sum;
            }
            case PRIMECOUNT -> {
                int count = 0;
                for (int n = job.getRangeStart(); n <= job.getRangeEnd(); n++) {
                    if (isPrime(n)) {
                        count++;
                    }
                }
                return count;
            }
            default -> throw new IllegalArgumentException("Unknown job type: " + job.getType());
        }
    }

    private static boolean isPrime(int n) {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;
        for (int i = 3; (long) i * i <= n; i += 2) {
            if (n % i == 0) return false;
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Coordinator-side: splitting a job into equal chunks across workers
    // ------------------------------------------------------------------

    /** Splits job evenly across every currently active worker and dispatches one chunk to each. */
    private void splitAndDispatch(Job job) throws RemoteException {
        if (!isCoordinator) {
            // Clients look up the coordinator before calling receiveJob, but
            // guard anyway in case the role changed in between (e.g. a term
            // limit just triggered a new election).
            throw new RemoteException("Worker " + id + " is not the coordinator; cannot accept a top-level job.");
        }
        if (bootstrap == null) {
            throw new RemoteException("Worker " + id + " has no Bootstrap reference; cannot look up active workers.");
        }

        onJobAssigned(); // once per top-level job, per the JAC/term-limit contract above

        List<Integer> activeWorkerIds = bootstrap.getActiveWorkerIDs();
        if (activeWorkerIds.isEmpty()) {
            activeWorkerIds = List.of(id); // degenerate case: just compute it ourselves
        }

        List<Job> chunks = splitIntoChunks(job, activeWorkerIds.size());
        List<ChunkAssignment> assignments = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            Job chunk = chunks.get(i);
            int targetWorkerId = activeWorkerIds.get(i);
            assignments.add(new ChunkAssignment(chunk, targetWorkerId));
            dispatchChunk(targetWorkerId, chunk);
        }

        System.out.println("[Worker " + id + "] Job " + job.getJobID() + " split into "
                + chunks.size() + " chunk(s) across workers " + activeWorkerIds);

        startPollingForCombinedResult(job, new JobDispatchState(assignments));
    }

    /** Divides job's workload into numChunks equally-sized (as possible) pieces, one per active worker. */
    private List<Job> splitIntoChunks(Job job, int numChunks) {
        List<Job> chunks = new ArrayList<>(numChunks);
        if (job.getType() == JobType.MAX) {
            List<Integer> numbers = job.getNumbers();
            int n = numbers.size();
            int base = n / numChunks;
            int remainder = n % numChunks;
            int index = 0;
            for (int i = 0; i < numChunks; i++) {
                int size = base + (i < remainder ? 1 : 0);
                List<Integer> sub = new ArrayList<>(numbers.subList(index, index + size));
                index += size;
                chunks.add(new Job(job.getJobID() + CHUNK_MARKER + i, JobType.MAX, sub, 0, 0));
            }
        } else {
            int total = job.getRangeEnd() - job.getRangeStart() + 1;
            int base = total / numChunks;
            int remainder = total % numChunks;
            int cursor = job.getRangeStart();
            for (int i = 0; i < numChunks; i++) {
                int size = base + (i < remainder ? 1 : 0);
                int chunkStart = cursor;
                int chunkEnd = chunkStart + size - 1; // if size == 0, chunkEnd < chunkStart -> empty range, contributes nothing
                cursor = chunkStart + size;
                chunks.add(new Job(job.getJobID() + CHUNK_MARKER + i, job.getType(), null, chunkStart, chunkEnd));
            }
        }
        return chunks;
    }

    /** Sends one chunk to its assigned worker (or runs it locally if that worker is this one). */
    private void dispatchChunk(int targetWorkerId, Job chunk) {
        if (targetWorkerId == this.id) {
            executeChunkLocally(chunk);
            return;
        }
        messageExecutor.submit(() -> {
            WorkerRMI targetStub = resolveStub(targetWorkerId);
            if (targetStub == null) {
                System.out.println("[Worker " + id + "] Could not dispatch chunk " + chunk.getJobID()
                        + " - worker " + targetWorkerId + " unreachable.");
                return;
            }
            try {
                targetStub.receiveJob(chunk);
            } catch (RemoteException e) {
                System.out.println("[Worker " + id + "] Failed to dispatch chunk " + chunk.getJobID()
                        + " to worker " + targetWorkerId + ": " + e.getMessage());
            }
        });
    }

    /**
     * One chunk's dispatch state: which worker currently has it, when it was
     * (most recently) sent, how many attempts have been made, and which
     * workers have already been tried (so a retry never lands on the same
     * dead worker twice). Mutated only from the single-threaded polling tick
     * for this job (see the note on scheduleAtFixedRate in startPollingForCombinedResult).
     */
    private static final class ChunkAssignment {
        final Job chunkJob;
        final String chunkId;
        final Set<Integer> triedWorkers = ConcurrentHashMap.newKeySet();
        int workerId;
        long dispatchedAtMillis;
        int attempts;

        ChunkAssignment(Job chunkJob, int workerId) {
            this.chunkJob = chunkJob;
            this.chunkId = chunkJob.getJobID();
            this.workerId = workerId;
            this.dispatchedAtMillis = System.currentTimeMillis();
            this.attempts = 1;
            this.triedWorkers.add(workerId);
        }
    }

    /** All of one job's chunk assignments, plus whether the job has been given up on entirely. */
    private static final class JobDispatchState {
        final List<ChunkAssignment> assignments;
        boolean failed = false;
        String failureReason;

        JobDispatchState(List<ChunkAssignment> assignments) {
            this.assignments = assignments;
        }
    }

    /**
     * Polls every assigned worker's getJobResult(chunkID) at a fixed interval
     * until all chunks for this job have reported in, then combines them and
     * stores the final result under the ORIGINAL jobID for clients to read.
     *
     * Also watches each chunk for CHUNK_TIMEOUT_MS of silence; on a timeout it
     * reassigns that chunk to a different active worker (up to MAX_CHUNK_RETRIES
     * times) or, if retries are exhausted or no other worker is available,
     * gives up on the whole job and records a failure JobResult instead.
     *
     * scheduleAtFixedRate never runs two ticks of the SAME task concurrently
     * (a slow tick just delays the next one), so mutating a job's own
     * ChunkAssignment state here without extra locking is safe; different
     * jobs' polling tasks never share state, so they don't interfere with
     * each other either.
     */
    private void startPollingForCombinedResult(Job originalJob, JobDispatchState state) {
        ScheduledFuture<?>[] futureHolder = new ScheduledFuture<?>[1];

        Runnable pollTask = () -> {
            List<Integer> partials = new ArrayList<>(state.assignments.size());
            boolean allDone = true;

            for (ChunkAssignment assignment : state.assignments) {
                JobResult r;
                try {
                    r = lookupChunkResult(assignment.workerId, assignment.chunkId);
                } catch (RemoteException e) {
                    r = null; // treat like "not ready yet"; the timeout check below is what actually detects a dead worker
                }

                if (r != null) {
                    partials.add(r.getResult());
                    continue;
                }

                allDone = false;
                long waitedMs = System.currentTimeMillis() - assignment.dispatchedAtMillis;
                if (waitedMs >= CHUNK_TIMEOUT_MS) {
                    handleChunkTimeout(state, assignment);
                    if (state.failed) {
                        break; // no point checking the rest - this job is done for
                    }
                }
            }

            if (state.failed) {
                finalJobResults.put(originalJob.getJobID(), JobResult.failure(originalJob.getJobID(), state.failureReason));
                System.out.println("[Worker " + id + "] Job " + originalJob.getJobID() + " FAILED: " + state.failureReason);
                futureHolder[0].cancel(false);
                return;
            }

            if (allDone) {
                int combined = combine(originalJob.getType(), partials);
                finalJobResults.put(originalJob.getJobID(), new JobResult(originalJob.getJobID(), combined, id));
                System.out.println("[Worker " + id + "] Job " + originalJob.getJobID() + " complete -> " + combined);
                futureHolder[0].cancel(false);
            }
            // else: some chunks still pending (and not yet timed out) - try again next tick
        };

        futureHolder[0] = jobPollScheduler.scheduleAtFixedRate(
                pollTask, JOB_POLL_INTERVAL_MS, JOB_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Called when a chunk has gone CHUNK_TIMEOUT_MS without a result. Either
     * reassigns it to a fresh worker and resets its clock, or - if retries are
     * exhausted or no untried active worker remains - marks the whole job failed.
     */
    private void handleChunkTimeout(JobDispatchState state, ChunkAssignment assignment) {
        System.out.println("[Worker " + id + "] Chunk " + assignment.chunkId + " timed out on worker "
                + assignment.workerId + " (attempt " + assignment.attempts + "/" + (MAX_CHUNK_RETRIES + 1) + ").");

        if (assignment.attempts > MAX_CHUNK_RETRIES) {
            state.failed = true;
            state.failureReason = "Chunk " + assignment.chunkId + " did not complete after "
                    + assignment.attempts + " attempt(s); last tried worker " + assignment.workerId + " was unresponsive.";
            return;
        }

        int replacementWorkerId = pickReplacementWorker(assignment.triedWorkers);
        if (replacementWorkerId == -1) {
            state.failed = true;
            state.failureReason = "Chunk " + assignment.chunkId + " timed out on worker " + assignment.workerId
                    + " and no other active worker is available to retry on.";
            return;
        }

        assignment.workerId = replacementWorkerId;
        assignment.attempts++;
        assignment.dispatchedAtMillis = System.currentTimeMillis();
        assignment.triedWorkers.add(replacementWorkerId);
        dispatchChunk(replacementWorkerId, assignment.chunkJob);
        System.out.println("[Worker " + id + "] Reassigned chunk " + assignment.chunkId + " to worker " + replacementWorkerId);
    }

    /** First currently-active worker that hasn't already been tried for this chunk, or -1 if none. */
    private int pickReplacementWorker(Set<Integer> excludeIds) {
        if (bootstrap == null) {
            return -1;
        }
        try {
            for (int candidateId : bootstrap.getActiveWorkerIDs()) {
                if (!excludeIds.contains(candidateId)) {
                    return candidateId;
                }
            }
        } catch (RemoteException e) {
            System.out.println("[Worker " + id + "] Could not query Bootstrap for a replacement worker: " + e.getMessage());
        }
        return -1;
    }

    private JobResult lookupChunkResult(int workerId, String chunkId) throws RemoteException {
        if (workerId == this.id) {
            return localChunkResults.get(chunkId);
        }
        WorkerRMI stub = resolveStub(workerId);
        if (stub == null) {
            return null; // unreachable this tick - the timeout check in the caller is what handles genuinely dead workers
        }
        return stub.getJobResult(chunkId);
    }

    private int combine(JobType type, List<Integer> partials) {
        switch (type) {
            case MAX -> {
                return partials.stream().mapToInt(Integer::intValue).max().orElse(Integer.MIN_VALUE);
            }
            case PRIMESUM, PRIMECOUNT -> {
                return partials.stream().mapToInt(Integer::intValue).sum();
            }
            default -> throw new IllegalArgumentException("Unknown job type: " + type);
        }
    }
}