package eu.europeana.batch.repository;

import static dev.morphia.query.Sort.descending;
import static dev.morphia.query.experimental.filters.Filters.eq;
import static dev.morphia.query.experimental.filters.Filters.in;
import static eu.europeana.batch.BatchConstants.*;

import com.mongodb.client.result.UpdateResult;
import dev.morphia.Datastore;
import dev.morphia.query.FindOptions;
import dev.morphia.query.experimental.updates.UpdateOperators;
import eu.europeana.batch.entity.JobExecutionEntity;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

@Repository
public class JobExecutionRepository extends AbstractRepository implements JobExecutionDao {

  public JobExecutionRepository(Datastore datastore) {
    super(datastore);
  }

  @Override
  public void saveJobExecution(JobExecution jobExecution) {
    validateJobExecution(jobExecution);
    jobExecution.incrementVersion();
    synchronized (this) {
      jobExecution.setId(generateSequence(JobExecutionEntity.class.getSimpleName()));
    }
    JobExecutionEntity jobExecutionEntity = JobExecutionEntity.toEntity(jobExecution);
    getDataStore().save(jobExecutionEntity);
  }

  /**
   * Update given JobExecution. The JobExecution is first checked to ensure all fields are not null,
   * and that it has an ID. The database is then queried to ensure that the ID exists, which ensures
   * that it is valid.
   *
   * @see JobExecutionDao#updateJobExecution(JobExecution)
   */
  @Override
  public void updateJobExecution(JobExecution jobExecution) {
    validateJobExecution(jobExecution);

    Long jobExecutionId = jobExecution.getId();
    Assert.notNull(
        jobExecutionId,
        "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
    Assert.notNull(
        jobExecution.getVersion(),
        "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

    synchronized (jobExecution) {
      int nextVersion = jobExecution.getVersion() + 1;

      // Check if given JobExecution's Id already exists, if none is found
      // it is invalid and
      // an exception should be thrown.
      if (getJobExecutionWithId(jobExecutionId) == null) {
        throw new NoSuchObjectException(
            "Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
      }

      UpdateResult result = queryUpdateJobExecution(jobExecution, jobExecutionId, nextVersion);

      // Avoid concurrent modifications
      if (result.getModifiedCount() == 0) {
        int currentVersion = queryGetJobExecutionVersion(jobExecutionId);
        throw new OptimisticLockingFailureException(
            "Attempt to update job execution id="
                + jobExecution.getId()
                + " with wrong version ("
                + jobExecution.getVersion()
                + "), where current version is "
                + currentVersion);
      }
    }
  }

  @Override
  public List<JobExecution> findJobExecutions(final JobInstance job) {
    Assert.notNull(job, "Job cannot be null.");
    Assert.notNull(job.getId(), "Job Id cannot be null.");

    return queryGetJobExecutions(job.getId()).stream()
        .map(JobExecutionEntity::fromEntity)
        .collect(Collectors.toList());
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(JobInstance jobInstance) {
    long id = jobInstance.getId();
    JobExecutionEntity executionEntity = queryGetLastJobExecutionForInstance(id);

    if (executionEntity == null) {
      return null;
    }

    return JobExecutionEntity.fromEntity(executionEntity);
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    List<Long> ids = getJobInstanceIdsWithName(jobName);
    List<JobExecutionEntity> jobExecutions = queryGetRunningJobExecutions(ids);

    return jobExecutions.stream().map(JobExecutionEntity::fromEntity).collect(Collectors.toSet());
  }

  @Nullable
  @Override
  public JobExecution getJobExecution(Long jobExecutionId) {
    return JobExecutionEntity.fromEntity(getJobExecutionWithId(jobExecutionId));
  }

  @Override
  public void synchronizeStatus(JobExecution jobExecution) {
    int currentVersion = queryGetJobExecutionVersion(jobExecution.getId());

    if (currentVersion != jobExecution.getVersion()) {
      String status = queryGetJobExecutionStatus(jobExecution.getId());
      jobExecution.upgradeStatus(BatchStatus.valueOf(status));
      jobExecution.setVersion(currentVersion);
    }
  }

  /**
   * Validate JobExecution. At a minimum, JobId, Status, CreateTime cannot be null.
   *
   * @param jobExecution
   * @throws IllegalArgumentException
   */
  private void validateJobExecution(JobExecution jobExecution) {
    Assert.notNull(jobExecution, "JobExecution cannot be null.");
    Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
    Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
    Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
  }

  /**
   * Gets the JobExecution version saved in the database.
   *
   * @param jobExecutionId
   * @return
   */
  private int queryGetJobExecutionVersion(long jobExecutionId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .iterator(new FindOptions().projection().include(VERSION_KEY).limit(1))
        .next()
        .getVersion();
  }

  private UpdateResult queryUpdateJobExecution(
      JobExecution jobExecution, Long jobExecutionId, int nextVersion) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(
            eq(JOB_EXECUTION_ID_KEY, jobExecutionId), eq(VERSION_KEY, jobExecution.getVersion()))
        .update(
            UpdateOperators.set(JOB_EXECUTION_ID_KEY, jobExecutionId),
            UpdateOperators.set(VERSION_KEY, nextVersion),
            UpdateOperators.set(JOB_INSTANCE_ID_KEY, jobExecution.getJobId()),
            handleNullField(START_TIME_KEY, jobExecution.getStartTime()),
            handleNullField(END_TIME_KEY, jobExecution.getEndTime()),
            UpdateOperators.set(STATUS_KEY, jobExecution.getStatus().toString()),
            UpdateOperators.set(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode()),
            UpdateOperators.set(
                EXIT_MESSAGE_KEY, jobExecution.getExitStatus().getExitDescription()),
            handleNullField(CREATE_TIME_KEY, jobExecution.getCreateTime()),
            handleNullField(LAST_UPDATED_KEY, jobExecution.getLastUpdated()))
        .execute();
  }

  private String queryGetJobExecutionStatus(long jobExecutionId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .iterator(new FindOptions().projection().include(STATUS_KEY).limit(1))
        .next()
        .getStatus();
  }

  private JobExecutionEntity queryGetLastJobExecutionForInstance(long jobInstanceId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_INSTANCE_ID_KEY, jobInstanceId))
        .iterator(new FindOptions().sort(descending(CREATE_TIME_KEY)).limit(1))
        .tryNext();
  }

  private List<JobExecutionEntity> queryGetRunningJobExecutions(final List<Long> jobInstanceIds) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(END_TIME_KEY, null), in(JOB_INSTANCE_ID_KEY, jobInstanceIds))
        .iterator(DESCENDING_JOB_EXECUTION)
        .toList();
  }
}
