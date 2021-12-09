package eu.europeana.batch.repository;

import static dev.morphia.query.Sort.ascending;
import static dev.morphia.query.Sort.descending;
import static dev.morphia.query.experimental.filters.Filters.eq;
import static dev.morphia.query.experimental.filters.Filters.in;
import static eu.europeana.batch.BatchConstants.*;

import com.mongodb.client.result.UpdateResult;
import dev.morphia.Datastore;
import dev.morphia.query.FindOptions;
import dev.morphia.query.experimental.updates.UpdateOperators;

import eu.europeana.batch.entity.JobExecutionEntity;
import eu.europeana.batch.entity.StepExecutionEntity;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class StepExecutionRepository extends AbstractRepository implements StepExecutionDao {

  public StepExecutionRepository(Datastore datastore) {
    super(datastore);
  }

  @Override
  public void saveStepExecution(StepExecution stepExecution) {
    prepareForSaving(stepExecution);
    StepExecutionEntity stepExecutionEntity = StepExecutionEntity.toEntity(stepExecution);
    getDataStore().save(stepExecutionEntity);
  }

  @Override
  public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save an null collect of step executions");

    // first generate IDs, increment versions, etc
    // TODO: ID generation requires DB lookup for each stepExecution. See if there's an efficient
    // way of doing this
    for (StepExecution stepExecution : stepExecutions) {
      prepareForSaving(stepExecution);
    }

    List<StepExecutionEntity> entities =
        stepExecutions.stream().map(StepExecutionEntity::toEntity).collect(Collectors.toList());

    getDataStore().save(entities, BATCH_INSERT_OPTIONS);
  }

  @Override
  public void updateStepExecution(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Assert.notNull(
        stepExecution.getId(),
        "StepExecution Id cannot be null. StepExecution must saved" + " before it can be updated.");

    synchronized (stepExecution) {
      int nextVersion = stepExecution.getVersion() + 1;

      UpdateResult result = queryUpdateStepExecution(stepExecution, nextVersion);

      // Avoid concurrent modifications
      if (result.getModifiedCount() == 0) {
        int currentVersion = queryGetStepExecutionVersion(stepExecution.getId());
        throw new OptimisticLockingFailureException(
            "Attempt to update step execution id="
                + stepExecution.getId()
                + " with wrong version ("
                + stepExecution.getVersion()
                + "), where current version is "
                + currentVersion);
      }

      stepExecution.incrementVersion();
    }
  }

  /**
   * Retrieve the last {@link StepExecution} for a given {@link JobInstance} ordered by starting
   * time and then id.
   *
   * @param jobInstance the parent {@link JobInstance}
   * @param stepName the name of the step
   * @return a {@link StepExecution}
   */
  @Nullable
  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    long jobInstanceId = jobInstance.getId();

    // get all job executions for this jobInstance
    List<JobExecutionEntity> jobExecutions = queryGetJobExecutions(jobInstanceId);

    if (jobExecutions.isEmpty()) {
      return null;
    }

    // Map JobExecutionID to jobExecution, so we can retrieve the matching JobExecution later
    Map<Long, JobExecutionEntity> jobExecutionMap =
        jobExecutions.stream()
            .collect(
                Collectors.toMap(
                    JobExecutionEntity::getJobExecutionId,
                    jobExecutionEntity -> jobExecutionEntity));

    List<StepExecutionEntity> stepExecutions =
        queryGetStepExecutions(jobExecutionMap.keySet(), stepName);

    if (stepExecutions.isEmpty()) {
      return null;
    }

    // list is already sorted by most recent first
    StepExecutionEntity stepExecution = stepExecutions.get(0);

    JobExecutionEntity jobExecution = jobExecutionMap.get(stepExecution.getJobExecutionId());

    return StepExecutionEntity.fromEntity(
        stepExecution, JobExecutionEntity.fromEntity(jobExecution));
  }

  @Override
  public int countStepExecutions(JobInstance jobInstance, String stepName) {
    long jobInstanceId = jobInstance.getId();

    // get all job executionIds for this jobInstance
    List<Long> jobExecutionId = queryGetJobExecutionIds(jobInstanceId);

    if (jobExecutionId.isEmpty()) {
      return 0;
    }

    return (int) queryCountStepExecutions(jobExecutionId, stepName);
  }

  @Nullable
  @Override
  public StepExecution getStepExecution(JobExecution jobExecution, @NonNull Long stepExecutionId) {
    List<StepExecutionEntity> instances =
        queryGetStepExecutions(jobExecution.getId(), stepExecutionId);
    Assert.state(
        instances.size() <= 1,
        "There can be at most one step execution with given name for single job execution");

    return StepExecutionEntity.fromEntity(instances.get(0), jobExecution);
  }

  @Override
  public void addStepExecutions(JobExecution jobExecution) {
    List<StepExecutionEntity> results =
        queryGetStepExecutionsWithJobExecutionId(jobExecution.getId());

    for (StepExecutionEntity entity : results) {
      // this calls the constructor of StepExecution, which adds it to the jobExecution
      StepExecutionEntity.fromEntity(entity, jobExecution);
    }
  }

  private void validateStepExecution(StepExecution stepExecution) {
    Assert.notNull(stepExecution, "StepExecution cannot be null.");
    Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
    Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
    Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
  }

  private void prepareForSaving(StepExecution stepExecution) {
    Assert.isNull(
        stepExecution.getId(),
        "to-be-saved (not updated) StepExecution can't already have an id assigned");
    Assert.isNull(
        stepExecution.getVersion(),
        "to-be-saved (not updated) StepExecution can't already have a version assigned");

    validateStepExecution(stepExecution);

    synchronized (this) {
      stepExecution.setId(generateSequence(StepExecutionEntity.class.getSimpleName()));
    }
    stepExecution.incrementVersion();
  }

  /**
   * Gets the StepExecution version saved in the database.
   *
   * <p>TODO: similar to JobExecutionRepository.queryGetJobExecutionVersion(). Refactor
   *
   * @param stepExecutionId
   * @return
   */
  private int queryGetStepExecutionVersion(long stepExecutionId) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(eq(STEP_EXECUTION_ID_KEY, stepExecutionId))
        .iterator(new FindOptions().projection().include(VERSION_KEY).limit(1))
        .next()
        .getVersion();
  }

  private List<StepExecutionEntity> queryGetStepExecutions(
      long jobExecutionId, long stepExecutionId) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(
            eq(STEP_EXECUTION_ID_KEY, stepExecutionId), eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .iterator()
        .toList();
  }

  private List<StepExecutionEntity> queryGetStepExecutionsWithJobExecutionId(long jobExecutionId) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .iterator(new FindOptions().sort(ascending(STEP_EXECUTION_ID_KEY)))
        .toList();
  }

  private UpdateResult queryUpdateStepExecution(StepExecution stepExecution, int newVersion) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(
            eq(STEP_EXECUTION_ID_KEY, stepExecution.getId()),
            eq(VERSION_KEY, stepExecution.getVersion()))
        .update(
            UpdateOperators.set(STEP_EXECUTION_ID_KEY, stepExecution.getId()),
            UpdateOperators.set(STEP_NAME_KEY, stepExecution.getStepName()),
            UpdateOperators.set(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId()),
            handleNullField(START_TIME_KEY, stepExecution.getStartTime()),
            handleNullField(END_TIME_KEY, stepExecution.getEndTime()),
            handleNullField(LAST_UPDATED_KEY, stepExecution.getLastUpdated()),
            UpdateOperators.set(STATUS_KEY, stepExecution.getStatus().toString()),
            UpdateOperators.set(COMMIT_COUNT_KEY, stepExecution.getCommitCount()),
            UpdateOperators.set(READ_COUNT_KEY, stepExecution.getReadCount()),
            UpdateOperators.set(FILTER_COUNT_KEY, stepExecution.getFilterCount()),
            UpdateOperators.set(WRITE_COUNT_KEY, stepExecution.getWriteCount()),
            UpdateOperators.set(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode()),
            UpdateOperators.set(
                EXIT_MESSAGE_KEY, stepExecution.getExitStatus().getExitDescription()),
            UpdateOperators.set(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount()),
            UpdateOperators.set(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount()),
            UpdateOperators.set(PROCESS_SKIP_COUNT_KEY, stepExecution.getProcessSkipCount()),
            UpdateOperators.set(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount()),
            UpdateOperators.set(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount()),
            handleNullField(VERSION_KEY, newVersion))
        .execute();
  }

  /**
   * Gets all StepExecutions for the given JobExecution Ids, with the step name. Results are sorted
   * in descending order of StartTime and StepExecutionID
   *
   * @param jobExecutionIds list of JobExecution Ids
   * @param stepName step name
   * @return list of StepExecution
   */
  private List<StepExecutionEntity> queryGetStepExecutions(
      Iterable<Long> jobExecutionIds, String stepName) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(eq(STEP_NAME_KEY, stepName), in(JOB_EXECUTION_ID_KEY, jobExecutionIds))
        .iterator(
            new FindOptions().sort(descending(START_TIME_KEY), descending(STEP_EXECUTION_ID_KEY)))
        .toList();
  }

  /**
   * Counts step executions in the given JobExecutions
   *
   * @param jobExecutionIds jobExecutionIds
   * @param stepName step name
   * @return number of stepExecutions
   */
  private long queryCountStepExecutions(Iterable<Long> jobExecutionIds, String stepName) {
    return getDataStore()
        .find(StepExecutionEntity.class)
        .filter(eq(STEP_NAME_KEY, stepName), in(JOB_EXECUTION_ID_KEY, jobExecutionIds))
        .count();
  }
}
