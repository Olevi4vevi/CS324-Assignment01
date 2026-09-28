package Common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface BootstrapRMI extends Remote {
    /**
     * Registers a newly-started worker and assigns it a unique ID.
     * The server (Bootstrap) owns ID assignment; workers never pick their own.
     */
    int registerWorker(WorkerRMI workerStub) throws RemoteException;

    /**
     * Returns the ID of a random active worker (excluding excludeWorkerID) to use
     * as an initial neighbor for a newly-joined worker, or -1 if none are available.
     * Only the ID is returned (not the stub) - the caller resolves the stub itself
     * via the RMI registry using the worker's binding name.
     */
    int getRandomWorkerID(int excludeWorkerID) throws RemoteException;

    List<Integer> getActiveWorkerIDs() throws RemoteException;
}
