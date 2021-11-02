package eu.europeana.batch.entity;

import static eu.europeana.batch.BatchConstants.EXECUTION_CTX_ID_KEY;
import static eu.europeana.batch.BatchConstants.EXECUTION_CTX_TYPE_KEY;

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Field;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Index;
import dev.morphia.annotations.Indexes;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.bson.types.ObjectId;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.item.ExecutionContext;

@Entity("ExecutionContext")
@Indexes({@Index(fields = {@Field(EXECUTION_CTX_ID_KEY), @Field(EXECUTION_CTX_TYPE_KEY)})})
public class ExecutionContextEntity {
  @Id private ObjectId _id;

  private long executionId;

  private String serializedContext;

  private ExecutionContextEntityType type;

  public long getExecutionId() {
    return executionId;
  }

  public String getSerializedContext() {
    return serializedContext;
  }

  public ExecutionContextEntityType getType() {
    return type;
  }

  public ExecutionContextEntity() {
    // default empty constructor
  }

  public ExecutionContextEntity(
      ExecutionContextEntityType type, long executionId, String serializedContext) {
    this.type = type;
    this.executionId = executionId;
    this.serializedContext = serializedContext;
  }

  public static ExecutionContextEntity toEntity(
      ExecutionContextEntityType type, Long executionId, String serializedContext) {
    return new ExecutionContextEntity(type, executionId, serializedContext);
  }

  public static ExecutionContext fromEntity(
      ExecutionContextEntity entity, ExecutionContextSerializer serializer) {
    ExecutionContext executionContext = new ExecutionContext();

    if (entity == null) {
      return executionContext;
    }

    String serializedContext = entity.getSerializedContext();

    // reproduced from JdbcExecutionContextDao (in Spring batch core)
    Map<String, Object> map;
    try {
      ByteArrayInputStream in =
          new ByteArrayInputStream(serializedContext.getBytes(StandardCharsets.UTF_8));
      map = serializer.deserialize(in);
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
    }
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      executionContext.put(entry.getKey(), entry.getValue());
    }
    return executionContext;
  }
}
