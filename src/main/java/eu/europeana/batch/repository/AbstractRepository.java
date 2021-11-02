package eu.europeana.batch.repository;

import static dev.morphia.query.experimental.filters.Filters.eq;
import static eu.europeana.batch.BatchConstants.DESCENDING_JOB_EXECUTION;
import static eu.europeana.batch.BatchConstants.JOB_EXECUTION_ID_KEY;
import static eu.europeana.batch.BatchConstants.JOB_INSTANCE_ID_KEY;
import static eu.europeana.batch.BatchConstants.JOB_NAME_KEY;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReturnDocument;
import dev.morphia.Datastore;
import dev.morphia.ModifyOptions;
import dev.morphia.query.FindOptions;
import dev.morphia.query.experimental.updates.UpdateOperator;
import dev.morphia.query.experimental.updates.UpdateOperators;
import dev.morphia.query.internal.MorphiaCursor;
import eu.europeana.batch.entity.JobExecutionEntity;
import eu.europeana.batch.entity.JobInstanceEntity;
import eu.europeana.batch.entity.SequenceGenerator;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRepository {

  public AbstractRepository(Datastore datastore) {
    this.datastore = datastore;
  }

  private Datastore datastore;

  protected Datastore getDataStore() {
    return this.datastore;
  }

  /**
   * Generates an autoincrement value for entities, based on the Entity type
   *
   * @param internalType internal type for Entity
   * @return autoincrement value
   */
  protected long generateSequence(String internalType) {
    // Get the given key from the auto increment entity and try to increment it.
    SequenceGenerator nextId =
        getDataStore()
            .find(SequenceGenerator.class)
            .filter(eq("_id", internalType))
            .modify(UpdateOperators.inc("value"))
            .execute(new ModifyOptions().returnDocument(ReturnDocument.AFTER));

    // If none is found, we need to create one for the given key.
    if (nextId == null) {
      nextId = new SequenceGenerator(internalType, 1L);
      getDataStore().save(nextId);
    }
    return nextId.getValue();
  }

  /**
   * Get JobInstanceIds with the given job name
   *
   * @param jobName jobName
   * @return List of jobInstance Ids
   */
  protected List<Long> getJobInstanceIdsWithName(String jobName) {
    List<Long> results = new ArrayList<>();
    MorphiaCursor<JobInstanceEntity> cursor =
        getDataStore()
            .find(JobInstanceEntity.class)
            .filter(eq(JOB_NAME_KEY, jobName))
            .iterator(new FindOptions().projection().include(JOB_INSTANCE_ID_KEY));

    while (cursor.hasNext()) {
      results.add(cursor.next().getJobInstanceId());
    }

    cursor.close();
    return results;
  }

  /**
   * Gets jobExecution IDs for the given job instance.
   *
   * @param jobInstanceId jobInstanceId
   * @return list of jobExecutionIds for JobInstance
   */
  protected List<Long> queryGetJobExecutionIds(long jobInstanceId) {
    List<Long> results = new ArrayList<>();

    MorphiaCursor<JobExecutionEntity> cursor =
        getDataStore()
            .find(JobExecutionEntity.class)
            .filter(eq(JOB_INSTANCE_ID_KEY, jobInstanceId))
            .iterator(new FindOptions().projection().include(JOB_EXECUTION_ID_KEY).limit(1));

    while (cursor.hasNext()) {
      results.add(cursor.next().getJobExecutionId());
    }
    cursor.close();
    return results;
  }

  /**
   * Gets all job executions for the given jobInstance, sorted by most recent first
   *
   * @param jobInstanceId jobInstanceId
   * @return List of JobExecutionEntities
   */
  protected List<JobExecutionEntity> queryGetJobExecutions(long jobInstanceId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_INSTANCE_ID_KEY, jobInstanceId))
        .iterator(DESCENDING_JOB_EXECUTION)
        .toList();
  }

  protected JobExecutionEntity getJobExecutionWithId(long jobExecutionId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .first();
  }

  protected long getJobExecutionInstanceId(long jobExecutionId) {
    return getDataStore()
        .find(JobExecutionEntity.class)
        .filter(eq(JOB_EXECUTION_ID_KEY, jobExecutionId))
        .iterator(new FindOptions().projection().include(JOB_INSTANCE_ID_KEY).limit(1))
        .next()
        .getJobInstanceId();
  }

  /**
   * Gets distinct values for a collection property
   *
   * @param clazz entity class
   * @return List containing distinct values
   */
  protected List<String> queryDistinctStringValues(final Class<?> clazz, final String fieldName) {
    DistinctIterable<String> iterable =
        getDataStore().getMapper().getCollection(clazz).distinct(fieldName, String.class);

    MongoCursor<String> cursor = iterable.iterator();
    List<String> result = new ArrayList<>();
    while (cursor.hasNext()) {
      result.add(cursor.next());
    }
    cursor.close();
    return result;
  }

  /**
   * Morphia doesn't allow updating fields with null values. They have to be unset instead. see:
   * https://groups.google.com/g/morphia/c/pcrpS4Gmdxw
   */
  protected UpdateOperator handleNullField(String fieldName, Object value) {
    return value == null ? UpdateOperators.unset(fieldName) : UpdateOperators.set(fieldName, value);
  }

  /**
   * Drops the Mongo collection for the specified Morphia entity
   *
   * @param clazz Morphia entity to clear collection for
   */
  protected void dropCollection(final Class<?> clazz) {
    getDataStore().getMapper().getCollection(clazz).drop();
  }
}
