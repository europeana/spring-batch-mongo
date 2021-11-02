package eu.europeana.batch.repository;

import static dev.morphia.query.Sort.descending;
import static dev.morphia.query.experimental.filters.Filters.eq;
import static dev.morphia.query.experimental.filters.Filters.or;
import static dev.morphia.query.experimental.filters.Filters.regex;
import static eu.europeana.batch.BatchConstants.JOB_INSTANCE_ID_KEY;
import static eu.europeana.batch.BatchConstants.JOB_KEY_GENERATOR;
import static eu.europeana.batch.BatchConstants.JOB_KEY_KEY;
import static eu.europeana.batch.BatchConstants.JOB_NAME_KEY;

import dev.morphia.Datastore;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filter;

import eu.europeana.batch.entity.JobInstanceEntity;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class JobInstanceRepository extends AbstractRepository implements JobInstanceDao {

  public JobInstanceRepository(Datastore datastore) {
    super(datastore);
  }

  @Override
  public JobInstance createJobInstance(final String jobName, final JobParameters jobParameters) {
    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");

    Assert.state(
        getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

    JobInstance jobInstance;
    synchronized (this) {
      long jobId = generateSequence(JobInstanceEntity.class.getSimpleName());
      jobInstance = new JobInstance(jobId, jobName);
    }
    jobInstance.incrementVersion();

    JobInstanceEntity jobInstanceEntity = JobInstanceEntity.toEntity(jobInstance, jobParameters);
    getDataStore().save(jobInstanceEntity);

    return jobInstance;
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(final String jobName, final JobParameters jobParameters) {
    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");

    String jobKey = JOB_KEY_GENERATOR.generateKey(jobParameters);
    List<JobInstanceEntity> instances = queryGetJobInstances(jobName, jobKey);

    Assert.state(
        instances.size() <= 1, "JobInstances cannot be more than 1. Was " + instances.size());

    if (instances.isEmpty()) {
      return null;
    }

    return JobInstanceEntity.fromEntity(instances.get(0));
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(Long instanceId) {
    return JobInstanceEntity.fromEntity(queryGetJobInstance(instanceId));
  }

  @Override
  public JobInstance getJobInstance(JobExecution jobExecution) {
    // get jobInstanceId for execution
    long instanceId = getJobExecutionInstanceId(jobExecution.getId());
    return getJobInstance(instanceId);
  }

  /**
   * Fetch the last job instances with the provided name, sorted backwards by primary key.
   *
   * @param jobName the job name
   * @param start the start index of the instances to return
   * @param count the maximum number of objects to return
   * @return the job instances with this name or empty if none
   */
  @Override
  public List<JobInstance> getJobInstances(String jobName, int start, int count) {
    return queryGetJobInstances(eq(JOB_NAME_KEY, jobName), start, count).stream()
        .map(JobInstanceEntity::fromEntity)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getJobNames() {
    return queryDistinctJobNames();
  }

  /**
   * Fetch the last job instances with the provided name, sorted backwards by primary key, using a
   * 'like' criteria
   *
   * @param jobName {@link String} containing the name of the job.
   * @param start int containing the offset of where list of job instances results should begin.
   * @param count int containing the number of job instances to return.
   * @return a list of {@link JobInstance} for the job name requested.
   */
  @Override
  public List<JobInstance> findJobInstancesByName(
      String jobName, final int start, final int count) {
    // create a regex pattern to match on *jobname*;
    return queryGetJobInstances(regex(JOB_NAME_KEY).pattern(".*" + jobName + ".*"), start, count)
        .stream()
        .map(JobInstanceEntity::fromEntity)
        .collect(Collectors.toList());
  }

  @Override
  public int getJobInstanceCount(String jobName) throws NoSuchJobException {
    long count = queryCountJobInstances(jobName);

    if (count == 0) {
      throw new NoSuchJobException("No job instances were found for job name " + jobName);
    }

    return (int) count;
  }

  private JobInstanceEntity queryGetJobInstance(long jobInstanceId) {
    return getDataStore()
        .find(JobInstanceEntity.class)
        .filter(eq(JOB_INSTANCE_ID_KEY, jobInstanceId))
        .first();
  }

  /**
   * Gets JobInstanceEntities matching the provided query parameters. Results are sorted with most
   * recent first.
   *
   * @param jobNameFilter Filter to use in query
   * @param start number of records to skip
   * @param count limit
   * @return List containing JobInstanceEntities
   */
  private List<JobInstanceEntity> queryGetJobInstances(Filter jobNameFilter, int start, int count) {
    return getDataStore()
        .find(JobInstanceEntity.class)
        .filter(jobNameFilter)
        .iterator(new FindOptions().sort(descending(JOB_INSTANCE_ID_KEY)).skip(start).limit(count))
        .toList();
  }

  private List<JobInstanceEntity> queryGetJobInstances(String jobName, String jobKey) {
    final Query<JobInstanceEntity> query = getDataStore().find(JobInstanceEntity.class);

    // if jobKey is empty, then return only jobs with an empty key
    if (StringUtils.hasLength(jobKey)) {
      query.filter(eq(JOB_KEY_KEY, jobKey));
    } else {
      query.filter(or(eq(JOB_KEY_KEY, jobKey), eq(JOB_KEY_KEY, null)));
    }
    return query.filter(eq(JOB_NAME_KEY, jobName)).iterator().toList();
  }

  /**
   * Fetch the last job instance by Id for the given job.
   *
   * @param jobName name of the job
   * @return the last job instance by Id if any or null otherwise
   */
  @Override
  @Nullable
  public JobInstance getLastJobInstance(@NonNull String jobName) {
    List<JobInstanceEntity> instances = queryGetJobInstances(eq(JOB_NAME_KEY, jobName), 0, 1);

    if (instances == null || instances.isEmpty()) {
      return null;
    }

    return JobInstanceEntity.fromEntity(instances.get(0));
  }

  /**
   * Gets all unique job names in database. TODO: use "distinct" or some other more efficient way
   * See https://github.com/MorphiaOrg/morphia/issues/219
   *
   * @return
   */
  private List<String> queryDistinctJobNames() {
    return queryDistinctStringValues(JobInstanceEntity.class, JOB_NAME_KEY);
  }

  private long queryCountJobInstances(String jobName) {
    return getDataStore().find(JobInstanceEntity.class).filter(eq(JOB_NAME_KEY, jobName)).count();
  }
}
