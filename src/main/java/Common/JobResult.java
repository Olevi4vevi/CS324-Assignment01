package Common;

import java.io.Serializable;

public class JobResult implements Serializable {
    private final String jobID;
    private final int result;
    private final int workerID;
    private final boolean failed;
    private final String errorMessage;
    
    //successful result constructor
    public JobResult(String jobID, int result, int workerID) {
        this.jobID = jobID;
        this.result = result;
        this.workerID = workerID;
        this.failed = false;
        this.errorMessage = null;
    }

    //failed result constructor
    private JobResult(String jobID, int result, int workerID, boolean failed, String errorMessage) {
        this.jobID = jobID;
        this.result = result;
        this.workerID = workerID;
        this.failed = failed;
        this.errorMessage = errorMessage;
    }

    public static JobResult failure(String jobID, String errorMessage) {
        return new JobResult(jobID, 0, -1, true, errorMessage);
    }

    public String getJobID() {
        return jobID;
    }

    public int getResult() {
        return result;
    }

    public int getWorkerID() {
        return workerID;
    }

    public boolean isFailed() {
        return failed;
    }
 
    /** Null unless isFailed() is true. */
    public String getErrorMessage() {
        return errorMessage;
    }
}