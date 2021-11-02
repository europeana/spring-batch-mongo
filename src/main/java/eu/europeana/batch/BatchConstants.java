package eu.europeana.batch;

import static dev.morphia.query.Sort.descending;

import dev.morphia.InsertManyOptions;
import dev.morphia.query.FindOptions;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;

public class BatchConstants {

  public static final String VERSION_KEY = "version";

  public static final String START_TIME_KEY = "startTime";
  public static final String END_TIME_KEY = "endTime";
  public static final String EXIT_CODE_KEY = "exitCode";
  public static final String EXIT_MESSAGE_KEY = "exitMessage";
  public static final String LAST_UPDATED_KEY = "lastUpdated";
  public static final String STATUS_KEY = "status";

  // Job Constants
  public static final String JOB_NAME_KEY = "jobName";
  public static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";
  public static final String JOB_KEY_KEY = "jobKey";

  public static final String DOT_ESCAPE_STRING = "\\-";
  public static final String DOT_STRING = "\\.";

  // Job Execution Constants
  public static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";
  public static final String CREATE_TIME_KEY = "createTime";

  // Job Execution Contexts Constants
  public static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";

  public static final String EXECUTION_CTX_ID_KEY = "executionId";
  public static final String EXECUTION_CTX_TYPE_KEY = "type";
  public static final String EXECUTION_CTX_SERIALIZED_KEY = "serializedContext";

  // Step Execution Constants
  public static final String STEP_NAME_KEY = "stepName";
  public static final String COMMIT_COUNT_KEY = "commitCount";
  public static final String READ_COUNT_KEY = "readCount";
  public static final String FILTER_COUNT_KEY = "filterCount";
  public static final String WRITE_COUNT_KEY = "writeCount";
  public static final String READ_SKIP_COUNT_KEY = "readSkipCount";
  public static final String WRITE_SKIP_COUNT_KEY = "writeSkipCount";
  public static final String PROCESS_SKIP_COUNT_KEY = "processSkipCount";
  public static final String ROLLBACK_COUNT_KEY = "rollbackCount";

  public static final JobKeyGenerator<JobParameters> JOB_KEY_GENERATOR =
      new DefaultJobKeyGenerator();
  public static final InsertManyOptions BATCH_INSERT_OPTIONS =
      new InsertManyOptions().ordered(true);
  public static final FindOptions DESCENDING_JOB_EXECUTION =
      new FindOptions().sort(descending(JOB_EXECUTION_ID_KEY));

  public static final String DATA_STORE_BEAN_NAME = "mongoBatchDataSource";
}
