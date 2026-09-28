package Client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import Common.BootstrapRMI;
import Common.Constants;
import Common.Job;
import Common.JobResult;
import Common.WorkerRMI;

/*
    Client  
*/
public class ClientGUIService {

    private static final int SUBMIT_RETRY_ATTEMPTS_MAX = 10;
    private static final long SUBMIT_RETRY_DELAY_MS = 1000;
    private static final long POLL_INTERVAL_MS = 1000;

    private final ExecutorService workerPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService pollingService = Executors.newScheduledThreadPool(4);
    private final ClientEventListener listener;

    private volatile BootstrapRMI bootstrap;
    private volatile Registry registry;

    public ClientGUIService(ClientEventListener listener) {
        this.listener = listener;    
    }

    public boolean isConnected() {
        return bootstrap != null && registry != null;
    }

    
    public void connect(String host, int port) {
        workerPool.submit(() -> {
            try {
                Registry reg = LocateRegistry.getRegistry(host, port);
                BootstrapRMI bs = (BootstrapRMI) reg.lookup(Constants.BOOTSTRAP_NAME);
                registry = reg;
                bootstrap = bs;
                listener.onConnected(host, port);
            } catch (RemoteException | NotBoundException ex) {
                listener.onConnectionFailed(ex.getMessage());
            }
        });
    }

    public void submitJob(Job job) {
        BootstrapRMI bootstrapRef = this.bootstrap;
        Registry registryRef = this.registry;
        if (bootstrapRef == null || registryRef == null) {
            listener.onJobStatusChange(job.getJobID(), "Not Connected");
            return;
        }

        workerPool.submit(() -> {
            try {
                WorkerRMI coordinator = findCoordinator(bootstrapRef, registryRef);
                if (coordinator == null) {
                    listener.onJobStatusChange(job.getJobID(), "No coordinator available yet");
                    return;
                }

                coordinator.receiveJob(job);
                listener.onJobStatusChange(job.getJobID(), "Running (polling)...");
                pollForResult(coordinator, job.getJobID());
            } catch (RemoteException ex) {
                listener.onJobStatusChange(job.getJobID(), "Failed: " + ex.getMessage());
            }
        });
    }

    private void attemptSubmit(Job job, BootstrapRMI bootstrapRef, Registry registryRef, int attempt) {
        try {
            WorkerRMI coordinator = findCoordinator(bootstrapRef, registryRef);
            if (coordinator == null) {
                retryOrFail(job, bootstrapRef, registryRef, attempt, "No coordinator available yet");
                return;
            }
 
            coordinator.receiveJob(job);
            listener.onJobStatusChange(job.getJobID(), "Running (polling)...");
            pollForResult(coordinator, job.getJobID());
        } catch (RemoteException ex) {
            retryOrFail(job, bootstrapRef, registryRef, attempt, ex.getMessage());
        }
    }
 
    private void retryOrFail(Job job, BootstrapRMI bootstrapRef, Registry registryRef, int attempt, String reason) {
        if (attempt >= SUBMIT_RETRY_ATTEMPTS_MAX) {
            listener.onJobStatusChange(job.getJobID(), "Failed: " + reason);
            return;
        }
        listener.onJobStatusChange(job.getJobID(),
                "Coordinator changing, retrying... (" + (attempt + 1) + "/" + SUBMIT_RETRY_ATTEMPTS_MAX + ")");
        pollingService.schedule(() -> attemptSubmit(job, bootstrapRef, registryRef, attempt + 1),
                SUBMIT_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    private WorkerRMI findCoordinator(BootstrapRMI bootstrapRef, Registry registryRef) throws RemoteException {
        List<Integer> activeIDs = bootstrapRef.getActiveWorkerIDs();
        for (int id : activeIDs) {
            try {
                WorkerRMI worker = (WorkerRMI) registryRef.lookup(Constants.WORKER_NAME_PREFIX + id);
                int coordinatorID = worker.getCoordinatorID();
                if (coordinatorID != -1) {
                    return (WorkerRMI) registryRef.lookup(Constants.WORKER_NAME_PREFIX + coordinatorID);
                }
            } catch (RemoteException | NotBoundException ex) {
                // that worker is unreachable/unbound; try the next one
            }
        }
        return null;
    }

    private void pollForResult(WorkerRMI coordinator, String jobID) {
        ScheduledFuture<?>[] futureHolder = new ScheduledFuture<?>[1];
        
        Runnable pollForTask = () -> {
            try {
                JobResult result = coordinator.getJobResult(jobID);
                if (result != null) {
                    listener.onJobResult(result);
                    futureHolder[0].cancel(false);    
                }
            } catch (RemoteException ex) {
                listener.onJobStatusChange(jobID, "Failed while polling: " + ex.getMessage());
                futureHolder[0].cancel(false);
            }
        };

        futureHolder[0] = pollingService.scheduleAtFixedRate(pollForTask, POLL_INTERVAL_MS,
             POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    
    }
}