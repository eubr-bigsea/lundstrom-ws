package bigsea;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.FileFilter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.io.File;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RestController
public class LundstromController extends Utilities{

  private final AtomicLong counter = new AtomicLong();

  // Executor service is needed to queue the calls to OPT_IC
  static final ExecutorService executor = Executors.newSingleThreadExecutor();

  @RequestMapping("/ws/lundstrom/{nNodes}/{nCores}/{ramGB}/{datasetSize}/{appId}")
  public String lundstrom(
    @PathVariable String nNodes,
    @PathVariable String nCores,
    @PathVariable String ramGB,
    @PathVariable String datasetSize,
    @PathVariable String appId
  ) throws java.io.IOException, Exception {

    String msg;

	  /* Read lundstrom location and logs folder from $HOME/wsi_config.xml */
    String resultsPath = readWsConfig("RESULTS_HOME");
    String lundstromPath = readWsConfig("LUNDSTROM_HOME");

    /* Check that the variables have been initialized or raise exception */
    checkConfigurationParameter(resultsPath, "Fatal error: LUNDSTROM_HOME not defined in wsi_config.xml ");
    checkConfigurationParameter(lundstromPath,  "Fatal error: RESULTS_HOME not defined in wsi_config.xml ");

    /* Build the folder path */
    String path = resultsPath + "/" + nNodes + "_" + nCores + "_" + ramGB + "_" + datasetSize +"/" + appId;

    /* Find the folder */
    File f = new File(path);

    /* If the folder doesn't exist, find the closest match among all the folders available */
    if (!f.exists())
    {
      // Get the list of all the folders
      File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
        @Override
        public boolean accept(File file)
        {
          return file.isDirectory();
        }
      });

      checkFoldersExistence(directories, "Fatal error: no sub-directories have been found in " + readWsConfig("RESULTS_HOME"));

      /* Find the closest folder to the input one */
      String reply = bestMatch(directories, nNodes, nCores, ramGB, datasetSize, appId);

      /* Update the number of cores and nodes to the the found folder */
      nNodes = reply.substring(0, reply.indexOf(" "));
      nCores = reply.substring(nNodes.length()+1, reply.length()-1);

    }

    String totalNcores = String.valueOf(Integer.valueOf(nCores) * Integer.valueOf(nNodes));

    try {
      Connection connection = readDataBase(
        readWsConfig("AppsPropDB_dbName"),
        readWsConfig("AppsPropDB_IP"),
        readWsConfig("AppsPropDB_user"),
        readWsConfig("AppsPropDB_pass")
      );
      connection.setAutoCommit(false);
      String dbName = readWsConfig("AppsPropDB_dbName");
      //ResultSet lookup_total_time = lookupLundstromStageRemainingTime(connection, dbName, appId, totalNcores, "0", datasetSize);
      ResultSet lookup_total_time = lookupLundstromStageRemainingTime(connection, dbName, appId, totalNcores, "0", ramGB);
      if (lookup_total_time == null || !lookup_total_time.next()) {
        msg = Start(lundstromPath, BuildLUA(resultsPath, nNodes, nCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNcores, datasetSize);
      }
      else {
        msg = String.valueOf((long)(lookup_total_time.getDouble("val")));
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    return msg;

  }


  /*
   * LUNDSTROM (reduced parameters number)
   */
  @RequestMapping("/ws/lundstromR/{nNodesnCores}/{ramGB}/{datasetSize}/{appId}")
    public String lundstromExtendedCall(
       @PathVariable String nNodesnCores,
       @PathVariable String ramGB,
       @PathVariable String datasetSize,
       @PathVariable String appId
    ) throws java.io.IOException, Exception {

      String msg = "";

      /* Read lundstrom location and logs folder from $HOME/wsi_config.xml */
      String resultsPath = readWsConfig("RESULTS_HOME");
      String lundstromPath = readWsConfig("LUNDSTROM_HOME");

      /* Check that the variables have been initialized or raise exception */
      checkConfigurationParameter(resultsPath, "Fatal error: LUNDSTROM_HOME not defined in wsi_config.xml ");
      checkConfigurationParameter(lundstromPath,  "Fatal error: RESULTS_HOME not defined in wsi_config.xml ");

      // Get the list of all the folders

      File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
        @Override
        public boolean accept(File file)
        {
          return file.isDirectory();
        }
      });


      String reply = bestMatchProduct(directories, nNodesnCores, ramGB, datasetSize, appId);

      String newNodes = reply.substring(0, reply.indexOf(" "));
      String newCores = reply.substring(newNodes.length()+1, reply.length());
      String totalNewNcores = String.valueOf(Integer.valueOf(newCores) * Integer.valueOf(newNodes));
      try {
        Connection connection = readDataBase(
          readWsConfig("AppsPropDB_dbName"),
          readWsConfig("AppsPropDB_IP"),
          readWsConfig("AppsPropDB_user"),
          readWsConfig("AppsPropDB_pass")
        );
        connection.setAutoCommit(false);
        String dbName = readWsConfig("AppsPropDB_dbName");
        msg = Start(lundstromPath, BuildLUA(resultsPath, newNodes, newCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNewNcores, datasetSize);
      }
      catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      return msg;
    }


  /*
   * OPT_IC
   */
  @RequestMapping("/ws/resopt/{appId}/{datasetSize}/{deadline}")
  public String Resopt(
    @PathVariable String appId,
    @PathVariable String datasetSize,
    @PathVariable String deadline
  ) throws java.io.IOException, Exception {

    if (readWsConfig("RESOPT_HOME") == null)
      return "Fatal error: RESOPT_HOME not defined in wsi_config.xml ";

    Connection connection = null;
    String result = "";

    try
    {
      connection = readDataBase(
        readWsConfig("AppsPropDB_dbName"),
        readWsConfig("AppsPropDB_IP"),
        readWsConfig("AppsPropDB_user"),
        readWsConfig("AppsPropDB_pass")
      );

      connection.setAutoCommit(false);

      // Create ConfigApp.txt
      createConfigAppFile(connection, appId, deadline);
      result = callResopt(connection, appId, datasetSize, deadline);
      close(connection);
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

    return result;
  }


  /*
   * LUNDSTROM CALL with stages
   */
  @RequestMapping("/ws/lundstrom/{nNodes}/{nCores}/{ramGB}/{datasetSize}/{appSessId}/{stage}/{currentTimeStamp}")
  public String lundstromCallStages(
    @PathVariable String nNodes,
    @PathVariable String nCores,
    @PathVariable String ramGB,
    @PathVariable String datasetSize,
    @PathVariable String appSessId,
    @PathVariable String stage,
    @PathVariable String currentTimeStamp
  ) throws java.io.IOException, Exception {
    String lundstromPath = readWsConfig("LUNDSTROM_HOME");
    String resultsPath = readWsConfig("RESULTS_HOME");

    if (lundstromPath == null)
      return "Fatal error: LUNDSTROM_HOME not defined in wsi_config.xml ";

    if (resultsPath == null)
      return "Fatal error: RESULTS_HOME not defined in wsi_config.xml ";

    // try connection with database first to retrieve application_id and submission time
    String dbName = readWsConfig("AppsPropDB_dbName");

    String appId;
    Timestamp submissionTime;
    try {
      Connection connection = readDataBase(
        readWsConfig("AppsPropDB_dbName"),
        readWsConfig("AppsPropDB_IP"),
        readWsConfig("AppsPropDB_user"),
        readWsConfig("AppsPropDB_pass")
      );

      connection.setAutoCommit(false);

      appId = retrieveAppId(appSessId, dbName, connection);
      submissionTime = retrieveSubmissionTime(appSessId, dbName, connection);

      assert (appId != null && submissionTime != null);

      // Find best match for nNodes and nCores
      String nNodesnCores = findBestMatch(resultsPath, nNodes, nCores, ramGB, datasetSize, appId);
      if (nNodesnCores == null) {
        // Could not find results folder
        throw new Exception("Error: Invalid nNodesnCores");
      }
      nNodes = nNodesnCores.substring(0, nNodesnCores.indexOf(" "));
      nCores = nNodesnCores.substring(nNodes.length()+1, nNodesnCores.length()-1);

      // Check lookup table
      long remainingTime, stage_end_time;

      String totalNcores = String.valueOf(Integer.valueOf(nCores) * Integer.valueOf(nNodes));
      ResultSet lookup_remaining_time = lookupLundstromStageRemainingTime(connection, dbName, appId, totalNcores, stage, datasetSize);
      ResultSet lookup_stage_end_time = lookupLundstromStageEndTime(connection, dbName, appId, totalNcores, stage, datasetSize);

      //if (lookup_result == null)
      if (lookup_remaining_time == null || !lookup_remaining_time.next() || lookup_stage_end_time == null || !lookup_stage_end_time.next())
      {
        // No previous run available, start lundstrom
        String lundstromOutput = StartLundstromStages(lundstromPath, BuildLUA(resultsPath, nNodes, nCores, ramGB, datasetSize, appId), connection, dbName, appId, totalNcores, datasetSize);

        String[] stages = getAllStages(lundstromOutput);
        boolean found = false;
        for (String s : stages) {
          if (s.equals(stage))
            found = true;
        }

        if (found == false)
          return "Stage " + stage + " does not exist";

        remainingTime = getRemainingTime(lundstromOutput, stage);
        stage_end_time = getStageWaitTime(lundstromOutput, stage);

      }
      else {
        stage_end_time = (long)(lookup_stage_end_time.getDouble("val"));
        remainingTime = (long)(lookup_remaining_time.getDouble("val"));
      }

      Timestamp now = new Timestamp(Long.parseLong(currentTimeStamp));
      long elapsedTime = now.getTime() - submissionTime.getTime();

      long rescaledRemainingTime = Math.round(remainingTime * (float)elapsedTime / stage_end_time);

      return remainingTime + " " + rescaledRemainingTime;
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * RESOPT WITH STAGES
   */
  @RequestMapping("/ws/resopt/{appSessId}/{datasetSize}/{deadline}/{stage}/{currentTimeStamp}")
  public String ResoptStages(
    @PathVariable String appSessId,
    @PathVariable String datasetSize,
    @PathVariable String deadline,
    @PathVariable String stage,
    @PathVariable String currentTimeStamp
  ) throws java.io.IOException, Exception {

    String resoptHome = readWsConfig("RESOPT_HOME");
    if (resoptHome == null) {
      throw new Exception("Fatal error: RESOPT_HOME not defined in wsi_config.xml");
    }

    Connection connection = null;
    String result = "";

    String appId;
    Timestamp submissionTime;
    try {
      connection = readDataBase(
        readWsConfig("AppsPropDB_dbName"),
        readWsConfig("AppsPropDB_IP"),
        readWsConfig("AppsPropDB_user"),
        readWsConfig("AppsPropDB_pass")
      );

      connection.setAutoCommit(false);

      String dbName = readWsConfig("AppsPropDB_dbName");
      appId = retrieveAppId(appSessId, dbName, connection);
      submissionTime = retrieveSubmissionTime(appSessId, dbName, connection);

      long stageEndTime = getStageEndTimeRunningJob(connection, dbName, appSessId, datasetSize, stage);

      Timestamp now = new Timestamp(Long.parseLong(currentTimeStamp));
      long elapsedTime = now.getTime() - submissionTime.getTime();


      long rescaledDeadline = Math.round((Long.parseLong(deadline)) * ((float) stageEndTime / elapsedTime));

      if (elapsedTime >= Long.parseLong(deadline) || resoptCallInvalid(connection, dbName, appId, datasetSize, rescaledDeadline))
        return "Deadline too strict";

      rescaledDeadline = roundToThousands(rescaledDeadline);
      System.out.println("rescaledDeadline: " + rescaledDeadline + " = " + deadline + " * (" + stageEndTime + " / " + elapsedTime + ")");

      // Call OPT_IC with the rescaled deadline asynchronously
      result = callResopt(connection, appId, datasetSize, String.valueOf(rescaledDeadline));

      int newNumCores = Integer.parseInt(result.split(" ")[0]);
      int newNumVM = Integer.parseInt(result.split(" ")[1]);

      if (newNumCores <= 0 || newNumVM <= 0) {
        // If the interpolation result is non positive, then return the current number of cores
        int nCoresRunning = Integer.parseInt(retrieveNcoresRunningJob(connection, dbName, appSessId));
        newNumCores = nCoresRunning;
        newNumVM = (int)Math.ceil(nCoresRunning/4.0);
        result = String.valueOf(newNumCores) + " " + String.valueOf(newNumVM);
      }

      // Update RUNNING_APPLICATION_TABLE with the new value for num_cores
      //updateNumCoresRunningApplication(connection, dbName, appSessId, String.valueOf(newNumCores));

      result = result + " " + rescaledDeadline;
      close(connection);

    }
    catch(Exception e)
    {
      e.printStackTrace();
      throw e;
    }

    return result;
  }


    /**
     * First check the cache in the database to see if there is a previously computed value of lundstrom -s. If no such value is present, then
     * start lundstrom with option -s, saves data in the database and return stage end time.
     * @param connection
     * @param dbName
     * @param appSessId
     * @param datasetSize
     * @return estimated end time of the stage running with the number of cores specified in RUNNING_APPLICATION_TABLE.
     */
    private long getStageEndTimeRunningJob(Connection connection, String dbName, String appSessId, String datasetSize, String stage) throws SQLException {
      String nCoresRunning = retrieveNcoresRunningJob(connection, dbName, appSessId);
      String appId = retrieveAppId(appSessId, dbName, connection);


      ResultSet lookupLundstrom = lookupLundstromStageEndTime(connection, dbName, appId, nCoresRunning, stage, datasetSize);
      if (lookupLundstrom != null && lookupLundstrom.next()) {
        long stageEndTime = (long)lookupLundstrom.getDouble("val");
        return stageEndTime;
      }
      else {
        // No previous run available, start lundstrom
        String lundstromPath = readWsConfig("LUNDSTROM_HOME");
        String resultsPath = readWsConfig("RESULTS_HOME");

        File [] directories = new File(readWsConfig("RESULTS_HOME")).listFiles(new FileFilter() {
          @Override
          public boolean accept(File file)
          {
            return file.isDirectory();
          }
        });

        String reply = bestMatchProduct(directories, nCoresRunning, datasetSize, "NA", appId);
        System.out.println("bestMatchProduct of " + nCoresRunning + " is " + reply);
        String newNodes = reply.substring(0, reply.indexOf(" "));
        String newCores = reply.substring(newNodes.length()+1, reply.length());

        String luaPath = BuildLUAWithoutMethod(resultsPath, newNodes, newCores, datasetSize, appId);
        String totalNewNcores = String.valueOf(Integer.valueOf(newNodes) * Integer.valueOf(newCores));

        String lundstromOutput = StartLundstromStages(lundstromPath, luaPath, connection, dbName, appId, totalNewNcores, datasetSize);

        long stage_end_time = getStageWaitTime(lundstromOutput, stage);

        return stage_end_time;
      }
    }


    /**
     * Search in the table RUNNING_APPLICATION_TABLE for the number of cores with which the application is running
     * @param connection
     * @param dbName
     * @param appSessId
     * @return the actual number of cores for the application
     * @throws SQLException
     */
    private String retrieveNcoresRunningJob(Connection connection, String dbName, String appSessId) throws SQLException {
      String sqlStatement = "SELECT num_cores FROM " + dbName + ".RUNNING_APPLICATION_TABLE WHERE application_session_id = '" + appSessId + "'";

      ResultSet result = query(dbName, connection, sqlStatement);
      if (result != null && result.next())
        return result.getString("num_cores");
      else
        throw new SQLException("Could not retrieve num cores from database");
    }

    /**
     * Call resopt with the specified parameters. If a previous result is present in cache, then this will be returned.
     * Otherwise, a first approximation will be returned by interpolating the two closest matches and a background call to resopt will be issued.
     * Later calls will then have the computed value of resopt.
     * @param connection
     * @param appId
     * @param datasetSize
     * @param deadline
     * @return A space-separated string of type "<num_cores_opt> <num_vm_opt>" containing the result of resopt.
     */
    public String callResopt(Connection connection, String appId, String datasetSize, String deadline) {
      String sqlStatement = "select num_vm_opt, num_cores_opt from " + readWsConfig("AppsPropDB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE "+
         "where application_id='" +  appId   + "'" +
         " and dataset_size='" + datasetSize + "'" +
         " and deadline=" + deadline;

      ResultSet resultSet =  query(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
      String result = "";

      try {
        /* Check if the desired configuration already exists in the DB */
        if (resultSet.next())
        {
          result = resultSet.getInt("num_cores_opt") + " "+ resultSet.getInt("num_vm_opt");
        }
        else
        {
          Future<String> future = executor.submit(new ResoptCallable(appId, datasetSize, deadline));
          /* Find all the record matching app_id, datasetSize */
          /* select * from OPTIMIZER_CONFIGURATION_TABLE ORDER BY ABS(755000 - deadline) limit 2;*/
          sqlStatement = "SELECT * FROM " + readWsConfig("AppsPropDB_dbName") +".OPTIMIZER_CONFIGURATION_TABLE "+
            "WHERE application_id="+"'" + appId + "'" +
            " AND dataset_size='" + datasetSize + "'"+
            " AND num_cores_opt <> '0' " + "ORDER BY ABS(" + deadline + " - deadline) limit 2";

          resultSet =  query(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);

          int[] num_cores_opt = new int[2];
          int[] num_vm_opt = new int[2];
          double[] deadlines = new double[2];
          int index = 0;


          while (resultSet.next())
          {
            deadlines[index] = resultSet.getDouble("deadline");
            num_cores_opt[index] = resultSet.getInt("num_cores_opt");
            num_vm_opt[index] = resultSet.getInt("num_vm_opt");
            index++;
          }


          int numCores = (int)Math.round(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_cores_opt));
          int numVM = (int)Math.round(Interpolation(Double.valueOf(deadline), deadlines[0], deadlines[1], num_vm_opt));

          result = String.valueOf(numCores) + " " + String.valueOf(numVM);
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      return result;
    }

    private long roundToThousands(long rescaledDeadline) {
      return 1000*(long)Math.round((double)rescaledDeadline/1000);
    }

    private void updateNumCoresRunningApplication(Connection connection, String dbName, String appSessId, String newNumCores) {
      String query = "UPDATE " + dbName + ".RUNNING_APPLICATION_TABLE SET num_cores = '" + newNumCores + "' WHERE application_session_id = '" + appSessId + "'";

      try {
        insert(dbName, connection, query);
        connection.commit();
      }
      catch(Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * Check if there is a precomputed value for the rescaled deadline whose value is 0 (hence invalid)
     * @param connection
     * @param dbName
     * @param application_id
     * @param datasetSize
     * @param rescaledDeadline
     * @return
     */
    private boolean resoptCallInvalid(Connection connection, String dbName, String application_id, String datasetSize, long rescaledDeadline) {
      boolean ret = false;
      try {
        String sqlStatement = "select max(deadline) as maxDeadline from " + dbName + ".OPTIMIZER_CONFIGURATION_TABLE WHERE application_id = '" + application_id + "' and dataset_size = '" + datasetSize + "' and num_cores_opt = '0'";
        ResultSet result = query(dbName, connection, sqlStatement);
        if (result != null && result.next()) {
          long maxDeadline = Long.parseLong(result.getString("maxDeadline"));
          if (maxDeadline >= rescaledDeadline)
            ret = true;
        }
      }
      catch (Exception e) {

      }

      return ret;
    }
  }

  /**
   * This method will launch OPT_IC in a new thread
   */
  class ResoptCallable extends Utilities implements Callable {
    private final String deadline;
    private final String datasetSize;
    private final String appId;

    public ResoptCallable(String appId, String datasetSize, String deadline) {
      this.appId = appId;
      this.datasetSize = datasetSize;
      this.deadline = deadline;
    }

    @Override
    public String call() {
      String msg1 = "0 0";
      try {
        String cmd1 = "cd " + readWsConfig("RESOPT_HOME")+";./script.sh " +
          " " + appId + " " + datasetSize + " " + deadline;
        msg1 = _run(cmd1);

        /* Write on DB the new solution */
        String[] splited = msg1.split("\\s+");
        // Save to database only if the returned values are positive.

        String sqlStatement = "insert into " + readWsConfig("AppsPropDB_dbName") +
          ".OPTIMIZER_CONFIGURATION_TABLE(application_id, dataset_size, deadline, num_cores_opt, num_vm_opt) values (" +
          "'" + appId + "', '" + datasetSize + "'," + deadline + "," + splited[0] +"," + splited[1] + ")";

        Connection connection = readDataBase(
          readWsConfig("AppsPropDB_dbName"),
          readWsConfig("AppsPropDB_IP"),
          readWsConfig("AppsPropDB_user"),
          readWsConfig("AppsPropDB_pass")
        );

        connection.setAutoCommit(false);
        insert(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
        connection.commit();
      }
      catch (Exception e) {

      }
      return msg1;
    }

}
