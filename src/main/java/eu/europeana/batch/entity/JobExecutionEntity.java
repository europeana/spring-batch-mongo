package eu.europeana.batch.entity;

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Indexed;
import eu.europeana.batch.BatchRepositoryUtils;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.bson.types.ObjectId;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;

@Entity("JobExecution")
public class JobExecutionEntity {

  @Id private ObjectId _id;

  private int version;

  @Indexed private long jobExecutionId;

  @Indexed private long jobInstanceId;

  private Date startTime;

  private Date endTime;

  private String status;

  private String exitCode;

  private String exitMessage;

  private Date createTime;

  private Date lastUpdated;

  private Map<String, Object> jobParameters = new HashMap<>();

  public int getVersion() {
    return version;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public long getJobInstanceId() {
    return jobInstanceId;
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

  public String getExitCode() {
    return exitCode;
  }

  public String getExitMessage() {
    return exitMessage;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public Date getLastUpdated() {
    return lastUpdated;
  }

  public void setJobParameters(Map<String, Object> jobParameters) {
    this.jobParameters = jobParameters;
  }

  public Map<String, Object> getJobParameters() {
    return jobParameters;
  }

  public static JobExecutionEntity toEntity(JobExecution jobExecution) {
    JobExecutionEntity jobExecutionEntity = new JobExecutionEntity();

    Map<String, Object> paramMap =
        BatchRepositoryUtils.convertToMap(jobExecution.getJobParameters());

    jobExecutionEntity.version = jobExecution.getVersion();
    jobExecutionEntity.jobExecutionId = jobExecution.getId();
    jobExecutionEntity.startTime = jobExecution.getStartTime();
    jobExecutionEntity.endTime = jobExecution.getEndTime();
    jobExecutionEntity.status = jobExecution.getStatus().toString();
    jobExecutionEntity.exitCode = jobExecution.getExitStatus().getExitCode();
    jobExecutionEntity.exitMessage = jobExecution.getExitStatus().getExitDescription();
    jobExecutionEntity.jobParameters = paramMap;
    jobExecutionEntity.createTime = jobExecution.getCreateTime();
    jobExecutionEntity.lastUpdated = jobExecution.getLastUpdated();

    return jobExecutionEntity;
  }

  public static JobExecution fromEntity(JobExecutionEntity jobExecutionEntity) {
    if (jobExecutionEntity == null) {
      return null;
    }

    JobParameters jobParameters =
        BatchRepositoryUtils.convertToJobParameters(jobExecutionEntity.getJobParameters());

    JobExecution jobExecution =
        new JobExecution(jobExecutionEntity.getJobExecutionId(), jobParameters);
    jobExecution.setStartTime(jobExecutionEntity.getStartTime());
    jobExecution.setEndTime(jobExecutionEntity.getEndTime());
    jobExecution.setStatus(BatchStatus.valueOf(jobExecutionEntity.getStatus()));
    jobExecution.setExitStatus(new ExitStatus(jobExecutionEntity.getExitCode()));

    jobExecution.setCreateTime(jobExecutionEntity.getCreateTime());
    jobExecution.setLastUpdated(jobExecutionEntity.getLastUpdated());
    jobExecution.setVersion(jobExecutionEntity.getVersion());

    return jobExecution;
  }
}
