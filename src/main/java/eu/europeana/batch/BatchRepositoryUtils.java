package eu.europeana.batch;

import static eu.europeana.batch.BatchConstants.DOT_ESCAPE_STRING;
import static eu.europeana.batch.BatchConstants.DOT_STRING;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;

public class BatchRepositoryUtils {

  /**
   * Converts {@link JobParameters} to a Map
   *
   * @param jobParameters jobParameters
   */
  public static Map<String, Object> convertToMap(JobParameters jobParameters) {
    // first clean the parameters, as we can't have "." within mongo field names
    Map<String, JobParameter> jobParams = jobParameters.getParameters();
    Map<String, Object> paramMap = new HashMap<>(jobParams.size());
    for (Entry<String, JobParameter> entry : jobParams.entrySet()) {
      paramMap.put(
          entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
    }
    return paramMap;
  }

  public static JobParameters convertToJobParameters(Map<String, Object> originParams) {
    if (originParams == null || originParams.isEmpty()) {
      return new JobParameters();
    }

    Map<String, JobParameter> destParams = new HashMap<>();

    for (Entry<String, Object> param : originParams.entrySet()) {
      String key = param.getKey();
      Object value = param.getValue();

      JobParameter jobParameter = null;

      if (value.getClass().isAssignableFrom(String.class)) {
        jobParameter = new JobParameter(String.valueOf(value));
      } else if (value.getClass().isAssignableFrom(Long.class)) {
        jobParameter = new JobParameter((Long) value);
      } else if (value.getClass().isAssignableFrom(Double.class)) {
        jobParameter = new JobParameter((Double) value);
      } else if (value.getClass().isAssignableFrom(Date.class)) {
        jobParameter = new JobParameter((Date) value);
      }

      // only these types are supported, so no need to assert that jobParameter is null
      destParams.put(key, jobParameter);
    }

    return new JobParameters(destParams);
  }
}
