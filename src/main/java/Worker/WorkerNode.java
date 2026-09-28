package Worker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Common.BootstrapRMI;
import Common.Constants;
import Common.WorkerRMI;

/**
 * Entry point for a Worker process. Registers with the Bootstrap node to get
 * an assigned ID, binds itself in the shared RMI registry, and connects to a
 * single random active worker as its first neighbor.
 *
 * Usage: java Worker.WorkerNode [bootstrapHost]
 * (defaults to localhost; all nodes currently share one registry on port 1099)
 */
public class WorkerNode {

    public static void main(String[] args) {
        String bootstrapHost = args.length > 0 ? args[0] : "localhost";
        int port = Constants.RMI_REGISTRY_PORT;

        try {
            Registry registry = LocateRegistry.getRegistry(bootstrapHost, port);
            BootstrapRMI bootstrap = (BootstrapRMI) registry.lookup(Constants.BOOTSTRAP_NAME);

            WorkerImpl worker = new WorkerImpl();
            int assignedID = bootstrap.registerWorker(worker);
            worker.setAssignedID(assignedID);
            worker.setRegistry(registry);
            worker.setBootstrap(bootstrap);

            String workerName = bindingNameFor(assignedID);
            registry.rebind(workerName, worker);
            System.out.println("[Worker " + assignedID + "] Registered and bound as \"" + workerName + "\"");

            connectToRandomNeighbor(registry, bootstrap, worker, assignedID);

            System.out.println("[Worker " + assignedID + "] Ready. Neighbors: " + worker.getNeighborIDs());
        } catch (Exception e) {
            System.err.println("[Worker] Failed to start: " + e.getMessage());
        }
    }

    private static void connectToRandomNeighbor(Registry registry, BootstrapRMI bootstrap,
                                                 WorkerImpl worker, int myID) throws Exception {
        int neighborID = bootstrap.getRandomWorkerID(myID);
        if (neighborID == -1) {
            System.out.println("[Worker " + myID + "] No other active workers yet; joining as the first node.");
            worker.startElection();
            return;
        }

        WorkerRMI neighborStub = (WorkerRMI) registry.lookup(bindingNameFor(neighborID));

        // Bidirectional edge: both sides record each other's ID only. Each
        // side resolves the other's actual stub from the registry whenever
        // it needs to send it a message.
        neighborStub.addNeighborWorker(myID);
        worker.addNeighborWorker(neighborID);
    }

    private static String bindingNameFor(int workerID) {
        return Constants.WORKER_NAME_PREFIX + workerID;
    }
}
