package eu.europeana.batch.entity;

import static eu.europeana.batch.BatchConstants.JOB_EXECUTION_ID_KEY;
import static eu.europeana.batch.BatchConstants.VERSION_KEY;

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Field;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Index;
import dev.morphia.annotations.Indexed;
import dev.morphia.annotations.Indexes;
import java.util.Date;
import org.bson.types.ObjectId;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

@Entity("StepExecution")
@Indexes({
  @Index(fields = {@Field(JOB_EXECUTION_ID_KEY), @Field(VERSION_KEY)}),
})
public class StepExecutionEntity {
  @Id private ObjectId _id;

  @Indexed private long stepExecutionId;

  private long jobExecutionId;

  @Indexed private String stepName;

  private Date startTime;

  private Date endTime;
  private String status;
  private int commitCount;
  private int readCount;
  private int filterCount;
  private int writeCount;
  private String exitCode;
  private String exitMessage;
  private int readSkipCount;
  private int writeSkipCount;
  private int processSkipCount;
  private int rollbackCount;
  private Date lastUpdated;
  private int version;

  public long getStepExecutionId() {
    return stepExecutionId;
  }

  public String getStepName() {
    return stepName;
  }

  public Date getStartTime() {
    return startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public String getStatus() {
    return status;
  }

  public int getCommitCount() {
    return commitCount;
  }

  public int getReadCount() {
    return readCount;
  }

  public int getFilterCount() {
    return filterCount;
  }

  public int getWriteCount() {
    return writeCount;
  }

  public String getExitCode() {
    return exitCode;
  }

  public String getExitMessage() {
    return exitMessage;
  }

  public int getReadSkipCount() {
    return readSkipCount;
  }

  public int getWriteSkipCount() {
    return writeSkipCount;
  }

  public int getProcessSkipCount() {
    return processSkipCount;
  }

  public int getRollbackCount() {
    return rollbackCount;
  }

  public Date getLastUpdated() {
    return lastUpdated;
  }

  public int getVersion() {
    return version;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public static StepExecutionEntity toEntity(final StepExecution stepExecution) {
    StepExecutionEntity entity = new StepExecutionEntity();

    entity.stepExecutionId = stepExecution.getId();
    entity.stepName = stepExecution.getStepName();
    entity.jobExecutionId = stepExecution.getJobExecutionId();
    entity.startTime = stepExecution.getStartTime();
    entity.endTime = stepExecution.getEndTime();
    entity.status = stepExecution.getStatus().toString();
    entity.commitCount = stepExecution.getCommitCount();
    entity.readCount = stepExecution.getReadCount();
    entity.filterCount = stepExecution.getFilterCount();
    entity.writeCount = stepExecution.getWriteCount();
    entity.exitCode = stepExecution.getExitStatus().getExitCode();
    entity.exitMessage = stepExecution.getExitStatus().getExitDescription();
    entity.readSkipCount = stepExecution.getReadSkipCount();
    entity.writeSkipCount = stepExecution.getWriteSkipCount();
    entity.processSkipCount = stepExecution.getProcessSkipCount();
    entity.rollbackCount = stepExecution.getRollbackCount();
    entity.lastUpdated = stepExecution.getLastUpdated();
    entity.version = stepExecution.getVersion();

    return entity;
  }

  public static StepExecution fromEntity(StepExecutionEntity entity, JobExecution jobExecution) {
    if (entity == null) {
      return null;
    }

    // stepExecution is added to jobExecution in this constructor!
    StepExecution stepExecution =
        new StepExecution(entity.getStepName(), jobExecution, entity.getStepExecutionId());

    stepExecution.setStartTime(entity.getStartTime());
    stepExecution.setEndTime(entity.getEndTime());
    stepExecution.setStatus(BatchStatus.valueOf(entity.getStatus()));
    stepExecution.setCommitCount(entity.getCommitCount());
    stepExecution.setReadCount(entity.getReadCount());
    stepExecution.setFilterCount(entity.getFilterCount());
    stepExecution.setWriteCount(entity.getWriteCount());
    stepExecution.setExitStatus(new ExitStatus(entity.getExitCode(), entity.getExitMessage()));
    stepExecution.setReadSkipCount(entity.getReadSkipCount());
    stepExecution.setWriteSkipCount(entity.getWriteSkipCount());
    stepExecution.setProcessSkipCount(entity.getProcessSkipCount());
    stepExecution.setRollbackCount(entity.getRollbackCount());
    stepExecution.setLastUpdated(entity.getLastUpdated());
    stepExecution.setVersion(entity.getVersion());
    return stepExecution;
  }
}
