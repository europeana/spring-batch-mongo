package eu.europeana.batch.config;

import dev.morphia.Datastore;
import eu.europeana.batch.repository.ExecutionContextRepository;
import eu.europeana.batch.repository.JobExecutionRepository;
import eu.europeana.batch.repository.JobInstanceRepository;
import eu.europeana.batch.repository.StepExecutionRepository;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/** Configures Spring Batch to use Mongo DAO implementations */
public class MongoBatchConfigurer {

  private final ExecutionContextDao mongoExecutionContextDao;
  private final JobExecutionDao mongoJobExecutionDao;
  private final JobInstanceDao mongoJobInstanceDao;
  private final StepExecutionDao mongoStepExecutionDao;

  private final TaskExecutor taskExecutor;

  /**
   * Instantiates the Mongo DAO implementations with the provided datastore
   *
   * @param datastore Morphia datastore to use
   */
  public MongoBatchConfigurer(Datastore datastore, TaskExecutor taskExecutor) {
    this.mongoExecutionContextDao = new ExecutionContextRepository(datastore);
    this.mongoJobExecutionDao = new JobExecutionRepository(datastore);
    this.mongoJobInstanceDao = new JobInstanceRepository(datastore);
    this.mongoStepExecutionDao = new StepExecutionRepository(datastore);
    this.taskExecutor = taskExecutor;
  }


  public JobRepository getJobRepository() throws Exception {
    return new SimpleJobRepository(
        mongoJobInstanceDao, mongoJobExecutionDao, mongoStepExecutionDao, mongoExecutionContextDao);
  }

  public PlatformTransactionManager getTransactionManager() throws Exception {
    return new ResourcelessTransactionManager();
  }

  public JobLauncher getJobLauncher() throws Exception {
    TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(getJobRepository());
    jobLauncher.setTaskExecutor(taskExecutor);
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }


  public JobExplorer getJobExplorer() throws Exception {
    return new SimpleJobExplorer(
        mongoJobInstanceDao, mongoJobExecutionDao, mongoStepExecutionDao, mongoExecutionContextDao);
  }
}
