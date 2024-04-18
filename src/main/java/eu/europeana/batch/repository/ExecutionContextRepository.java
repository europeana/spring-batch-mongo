package eu.europeana.batch.repository;

import static dev.morphia.query.filters.Filters.eq;
import static eu.europeana.batch.BatchConstants.BATCH_INSERT_OPTIONS;
import static eu.europeana.batch.BatchConstants.EXECUTION_CTX_ID_KEY;
import static eu.europeana.batch.BatchConstants.EXECUTION_CTX_SERIALIZED_KEY;
import static eu.europeana.batch.BatchConstants.EXECUTION_CTX_TYPE_KEY;

import dev.morphia.Datastore;
import dev.morphia.query.updates.UpdateOperators;

import eu.europeana.batch.entity.ExecutionContextEntity;
import eu.europeana.batch.entity.ExecutionContextEntityType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

public class ExecutionContextRepository extends AbstractRepository implements ExecutionContextDao {

  public ExecutionContextRepository(Datastore datastore) {
    super(datastore);
  }

  private final ExecutionContextSerializer serializer =
      new Jackson2ExecutionContextStringSerializer();

  @Override
  public ExecutionContext getExecutionContext(JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    Assert.notNull(executionId, "ExecutionId must not be null.");

    ExecutionContextEntity ctxEntity =
        queryFindExecutionContext(ExecutionContextEntityType.JOB, executionId);
    return ExecutionContextEntity.fromEntity(ctxEntity, serializer);
  }

  @Override
  public ExecutionContext getExecutionContext(StepExecution stepExecution) {
    Long executionId = stepExecution.getId();
    Assert.notNull(executionId, "ExecutionId must not be null.");

    ExecutionContextEntity ctxEntity =
        queryFindExecutionContext(ExecutionContextEntityType.STEP, executionId);
    return ExecutionContextEntity.fromEntity(ctxEntity, serializer);
  }

  @Override
  public void saveExecutionContext(JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    ExecutionContext executionContext = jobExecution.getExecutionContext();
    querySaveExecutionContext(ExecutionContextEntityType.JOB, executionId, executionContext);
  }

  @Override
  public void saveExecutionContext(StepExecution stepExecution) {
    Long executionId = stepExecution.getId();
    ExecutionContext executionContext = stepExecution.getExecutionContext();
    querySaveExecutionContext(ExecutionContextEntityType.STEP, executionId, executionContext);
  }

  @Override
  public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save an null collection of step executions");
    List<ExecutionContextEntity> ctxEntities = new ArrayList<>(stepExecutions.size());
    for (StepExecution stepExecution : stepExecutions) {
      Long executionId = stepExecution.getId();
      ExecutionContext executionContext = stepExecution.getExecutionContext();
      Assert.notNull(executionId, "ExecutionId must not be null.");
      Assert.notNull(executionContext, "The ExecutionContext must not be null.");

      ctxEntities.add(
          new ExecutionContextEntity(
              ExecutionContextEntityType.STEP, executionId, serializeContext(executionContext)));
    }

    getDataStore().save(ctxEntities, BATCH_INSERT_OPTIONS);
  }

  @Override
  public void updateExecutionContext(final JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    ExecutionContext executionContext = jobExecution.getExecutionContext();
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    String serializedContext = serializeContext(executionContext);
    queryUpdateExecutionContext(ExecutionContextEntityType.JOB, executionId, serializedContext);
  }

  @Override
  public void updateExecutionContext(final StepExecution stepExecution) {
    synchronized (stepExecution) {
      Long executionId = stepExecution.getId();
      ExecutionContext executionContext = stepExecution.getExecutionContext();
      Assert.notNull(executionId, "ExecutionId must not be null.");
      Assert.notNull(executionContext, "The ExecutionContext must not be null.");

      String serializedContext = serializeContext(executionContext);
      queryUpdateExecutionContext(ExecutionContextEntityType.STEP, executionId, serializedContext);
    }
  }

  private void queryUpdateExecutionContext(
      ExecutionContextEntityType type, Long executionId, String serializedContext) {
    getDataStore()
        .find(ExecutionContextEntity.class)
        .filter(eq(EXECUTION_CTX_ID_KEY, executionId), eq(EXECUTION_CTX_TYPE_KEY, type.toString()))
        .update(UpdateOperators.set(EXECUTION_CTX_SERIALIZED_KEY, serializedContext))
        .execute();
  }

  private ExecutionContextEntity queryFindExecutionContext(
      ExecutionContextEntityType type, long executionId) {
    return getDataStore()
        .find(ExecutionContextEntity.class)
        .filter(eq(EXECUTION_CTX_TYPE_KEY, type.toString()), eq(EXECUTION_CTX_ID_KEY, executionId))
        .first();
  }

  private void querySaveExecutionContext(
      ExecutionContextEntityType type, Long executionId, ExecutionContext executionContext) {
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");
    String serializedContext = serializeContext(executionContext);
    getDataStore().save(ExecutionContextEntity.toEntity(type, executionId, serializedContext));
  }

  /**
   * Serializes the ExecutionContext Reproduced from {@link
   * org.springframework.batch.core.repository.dao.JdbcExecutionContextDao}
   */
  private String serializeContext(ExecutionContext ctx) {
    Map<String, Object> m = new HashMap<>();
    for (Map.Entry<String, Object> me : ctx.entrySet()) {
      m.put(me.getKey(), me.getValue());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String results;

    try {
      serializer.serialize(m, out);
      results = out.toString(StandardCharsets.UTF_8);
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Could not serialize the execution context", ioe);
    }

    return results;
  }
}
