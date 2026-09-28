package Client;

import Common.JobResult;

public interface ClientEventListener {
    void onConnected(String host, int port);
    void onConnectionFailed(String message);
    void onJobStatusChange(String jobID, String status);
    void onJobResult(JobResult result);
}
