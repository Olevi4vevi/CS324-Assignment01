package Bootstrap;

import Common.BootstrapRMI;
import Common.WorkerRMI;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks every currently registered active worker. Does not take part in
 * elections or job processing - it only hands out IDs and helps new workers
 * find a random existing worker to connect to.
 */
public class BootstrapImpl extends UnicastRemoteObject implements BootstrapRMI {

    private static final long HEARTBEAT_INTERVAL_SECONDS = 10;

    private final AtomicInteger nextWorkerID = new AtomicInteger(1);
    private final Map<Integer, WorkerRMI> activeWorkers = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    public BootstrapImpl() throws RemoteException {
        super();
        heartbeatExecutor.scheduleAtFixedRate(this::pruneDeadWorkers,
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public int registerWorker(WorkerRMI workerStub) throws RemoteException {
        int id = nextWorkerID.getAndIncrement();
        activeWorkers.put(id, workerStub);
        System.out.println("[Bootstrap] Registered worker " + id + " (" + activeWorkers.size() + " active)");
        return id;
    }

    @Override
    public int getRandomWorkerID(int excludeWorkerID) throws RemoteException {
        List<Integer> candidates = new ArrayList<>();
        for (Integer id : activeWorkers.keySet()) {
            if (id != excludeWorkerID) {
                candidates.add(id);
            }
        }
        if (candidates.isEmpty()) {
            return -1;
        }
        return candidates.get(random.nextInt(candidates.size()));
    }

    @Override
    public List<Integer> getActiveWorkerIDs() throws RemoteException {
        return new ArrayList<>(activeWorkers.keySet());
    }

    /** Periodically pings known workers and drops any that no longer respond. */
    private void pruneDeadWorkers() {
        for (Map.Entry<Integer, WorkerRMI> entry : activeWorkers.entrySet()) {
            try {
                entry.getValue().getID();
            } catch (RemoteException e) {
                System.out.println("[Bootstrap] Worker " + entry.getKey() + " unreachable, removing.");
                activeWorkers.remove(entry.getKey());
            }
        }
    }
}
