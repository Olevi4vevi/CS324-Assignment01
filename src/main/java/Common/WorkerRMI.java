package Common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface WorkerRMI extends Remote {
    int getID() throws RemoteException;
    int getJAC() throws RemoteException;

    // Neighbors are recorded by ID only; each worker resolves the actual
    // stub for a given ID via the shared RMI registry when it needs to send
    // to it, rather than caching a long-lived stub reference.
    void addNeighborWorker(int neighborID) throws RemoteException;
    List<Integer> getNeighborIDs() throws RemoteException;

    void receiveElection(ElectionMessage msg) throws RemoteException;
    void receiveCoordinatorMsg(CoordinatorMessage msg) throws RemoteException;

    void receiveJob(Job job) throws RemoteException; 

    int getCoordinatorID() throws RemoteException;
    JobResult getJobResult(String jobID) throws RemoteException;
}
