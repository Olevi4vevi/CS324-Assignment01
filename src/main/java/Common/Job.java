package Common;

import java.io.Serializable;
import java.util.List;

public class Job implements Serializable {
    private final String jobID;
    private final JobType type;
    private final List<Integer> numbers;
    private final int rangeStart;
    private final int rangeEnd;

    public Job(String jobID, JobType type, List<Integer> numbers, int rangeStart, int rangeEnd) {
        this.jobID = jobID;
        this.type = type;
        this.numbers = numbers;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public String getJobID() {
        return jobID;
    }

    public JobType getType() {
        return type;
    }

    public List<Integer> getNumbers() {
        return numbers;
    }

    public int getRangeStart() {
        return rangeStart;
    }

    public int getRangeEnd() {
        return rangeEnd;
    }

}
