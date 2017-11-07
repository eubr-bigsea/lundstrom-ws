/*##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
*/

package bigsea;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.io.FileNotFoundException;
import java.util.Enumeration;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Utilities {

   public ResultSet query(String dbName, Connection connect, String sqlStatement)
   {
      System.out.println("Query is: " + sqlStatement);
      ResultSet resultSet = null;
      try
      {
         Statement statement = connect.createStatement();

         resultSet = statement.executeQuery(sqlStatement);

      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
      return resultSet;
   }

   /*
    * checkConfigurationParameter
    *
    * Check that the value of the variable in wsi_config.xml file has a value
    */
   public void checkConfigurationParameter(String variable, String msg)
   {
	   try
	   {
		   if (variable == null) throw new Exception(msg);
	   }
	   catch(Exception e)
 	  {
 		  e.printStackTrace();
 		  System.exit(-1);
 	  }
   }

   /*
    * checkFoldersExistence
    * Check that the set of folders (data log) exists
    */
   public void checkFoldersExistence(File name[], String msg)
   {
	   try
	   {
		   if (name == null) throw new Exception(msg);
	   }
	   catch(Exception e)
 	  {
 		  e.printStackTrace();
 		  System.exit(-1);
 	  }
   }

   public void createConfigAppFile(Connection connection, String appId, String deadline) throws RuntimeException
   {
      String sqlStatement, configApp = new String();
      ResultSet resultSet = null;

      /*
       * 	Build the ConfigApp.txt
       * select application_id, chi_0, chi_c, phi_mem,vir_mem, phi_mem,phi_core, vir_core
       * from APPLICATION_PROFILE_TABLE
       *
       *
       */

      sqlStatement = "SELECT application_id, chi_0, chi_c, phi_mem,vir_mem, phi_core,vir_core FROM " + readWsConfig("AppsPropDB_dbName") +".APPLICATION_PROFILE_TABLE "+
         "WHERE application_id="+"'" + appId + "'";
      try
      {
         resultSet =  query(readWsConfig("AppsPropDB_dbName"), connection, sqlStatement);
         if (!resultSet.next())
         {
            throw new RuntimeException("Fatal error: SqlStatement " + sqlStatement+" returned 0 rows");


         }

         configApp = resultSet.getString("application_id").concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getDouble("chi_0"))).concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getDouble("chi_c"))).concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getDouble("phi_mem"))).concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getDouble("vir_mem"))).concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getInt("phi_core"))).concat(" ");
         configApp = configApp.concat(String.valueOf(resultSet.getInt("vir_core"))).concat(" ");
         configApp = configApp.concat(deadline);

      }
      catch(Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

      BufferedWriter writer = null;
      try
      {
         //create ConfigApp_1.txt file
         String filename = readWsConfig("RESOPT_HOME");
         File myFile = new File(filename.concat("/ConfigApp_1.txt"));


         writer = new BufferedWriter(new FileWriter(myFile));
         writer.write(configApp);
      }

      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
      finally
      {
         try
         {

            writer.close();
         } catch (Exception e)
         {
         }
      }


   }



   public void insert(String dbName, Connection connect, String sqlStatement)
   {
      System.out.println("Insert query is: " + sqlStatement);
      try
      {
         Statement statement = connect.createStatement();

         statement.executeUpdate(sqlStatement);

      }
      catch(Exception e)
      {
         e.printStackTrace();
      }

   }

   /* Temporary solution: avg not interpolation */
   public double Interpolation(double x, double x1, double x2, int[] values)
   {
      double res1;
      double res2;

      res1 = (x - x1)/(x2 - x1);
      res2 = values[1] - values[0];

      return values[0] + res1*res2;
   }


   public Connection readDataBase(String dbName, String Ip, String user, String password
         ) throws Exception
   {
      Connection connect = null;
      try
      {
         // This will load the MySQL driver, each DB has its own driver
         Class.forName("com.mysql.cj.jdbc.Driver");
         // Setup the connection with the DB
         String conn_string = "jdbc:mysql://" + Ip +"/" + dbName + "?"
            + "user=" + user + "&password="+ password;
         connect = DriverManager.getConnection(conn_string);

      }
      catch (Exception e)
      {
         throw e;
      }

      return connect;
   }

   public String writeResultSet(ResultSet resultSet) throws SQLException {
      String application_id = "";
      String dataset_size = "";
      String deadline = "";

      while (resultSet.next()) {

         application_id = resultSet.getString("application_id");
         dataset_size = resultSet.getString("dataset_size");
         deadline = resultSet.getString("deadline");

      }
      return application_id +" " + dataset_size + " "+ deadline;
   }


   public void close(Connection connect) {
      try {
         if (connect != null) {
            connect.close();
         }
      } catch (Exception e) {

      }
   }

   public String bestMatchProduct(File[] directories, String nNodesnCores, String dataset_size, String param, String app_id)
   {
      int min = Integer.MAX_VALUE;
      int diff = 0;
      int prod;
      String parameters = "";
      int newnNodes, newnCores;

      for (int i = 0; i < directories.length; i++)
      {
         newnNodes = extract(directories[i].getName(), 1);
         newnCores = extract(directories[i].getName(), 2);
         prod =  newnNodes * newnCores;
         diff = Math.abs(Integer.valueOf(nNodesnCores) - prod);
         if (min > diff)
         {
            parameters = String.valueOf(newnNodes) + " "+ String.valueOf(newnCores);
            min = diff;
         }
      }

      return parameters;
   }

   public String readWsConfig(String variable)
   {
      String filename = System.getenv("HOME") + "/" + "wsi_config.xml";

      try {
         File file = new File(filename);
         FileInputStream fileInput = new FileInputStream(file);
         Properties properties = new Properties();
         properties.loadFromXML(fileInput);
         fileInput.close();

         Enumeration enuKeys = properties.keys();
         while (enuKeys.hasMoreElements())
         {
            String key = (String) enuKeys.nextElement();
            String value = properties.getProperty(key);
            //System.out.println(key + ": " + value);
            if (key.equals(variable)) return(value);
         }
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }

      return("Error: variable not found in wsi_cnfig.xml file");
   }



   public String bestMatch(File[] directories, String nNodes, String nCores, String dataset_size, String param, String app_id)
   {

      int min = Integer.MAX_VALUE;
      int diff = 0;
      int position = 1;
      String pattern;

      pattern = nNodes + "_" + nCores + "_" + dataset_size + "_" + param +
         "/" + app_id;
      boolean  nodesFound = false, coresFound = false;
      int saveIndex[] = new int[2];
      String newNNodes = "", newNCores = "";

      String parameters = nNodes + " " + nCores + " ";



      // Find the best match. Consider first number of nNodes then nCores
      for (position = 0; position <2; position++)
      {
         min = Integer.MAX_VALUE;
         for (int i = 0; i < directories.length; i++)
         {

            diff =  Math.abs((extract(pattern, position+1) - extract(directories[i].getName(), position+1)));
            // diff == 0 means the two parameters match
            if (diff == 0)
            {
               if (position+1 == 1) nodesFound = true;
               else if (position+1 == 2) coresFound = true;
               else System.exit(-1);
               break; // pass to the other parameters or quit
            }
            else
               if (min > diff)
               {
                  saveIndex[position] = i;
                  min = diff;
               }
         }
      }

      // Prepare the new parameters
      if (!nodesFound) newNNodes = String.valueOf(extract(directories[saveIndex[0]].getName(), 1));
      if (!coresFound) newNCores = String.valueOf(extract(directories[saveIndex[1]].getName(), 2));

      if (coresFound && !nodesFound) // it means a match for nNodes has been found
         parameters = Integer.valueOf(newNNodes) + " " + Integer.valueOf(nCores) + " ";
      else // it means a match for nCores has been found
         if (!coresFound && nodesFound)  parameters = nNodes + " " + newNCores + " ";
         else // It means a match for nNodes has been found
            if (!coresFound && !nodesFound) parameters = newNNodes + " " + newNCores + " ";

      return parameters;
   }



   public String Start(String path, String luafilename, Connection connection, String dbName, String appId, String nCores, String datasetSize) throws Exception{
      String output;

      output = StartLundstromStages(path, luafilename, connection, dbName, appId, nCores, datasetSize);
      String result = String.valueOf(getTotalExecutionTime(output));

      String save_whole_time = "REPLACE INTO " + dbName + ".PREDICTOR_CACHE_TABLE(application_id, num_cores, stage, dataset_size, val, is_residual) VALUES ('" + appId + "', " + nCores + ", '" + "0" + "', " + datasetSize + ", " + result + ", TRUE)";

      insert(dbName, connection, save_whole_time);
      connection.commit();
      return result;
   }



   /**
    * Start lundstrom with the -s option
    * @param path path to lundstrom folder
    * @param luafilename path to the lua model description
    * @return The complete lundstrom output
    */
   public String StartLundstromStages(String path, String luafilename, Connection connection, String dbName, String appId, String nCores, String datasetSize) throws SQLException {
      // String cmd = "cd " + path + ";./lundstrom.sh " + luafilename + " -s 2>&1";
      String cmd = "cd " + path + ";python run.py " + luafilename;

      String outputMsg = _run(cmd);

      if (outputMsg.contains("ERROR"))
         return "ERROR: could not open lua file. Check -demo_properties file";
      else {
         // Save the results in lookup table for all the stages
         String[] stages = getAllStages(outputMsg);
         for (String s : stages) {
            saveLookupLundstromStages(connection, dbName, appId, nCores, s, datasetSize, getStageWaitTime(outputMsg, s), getRemainingTime(outputMsg, s));
         }
         return outputMsg;
      }
   }

   /**
    * Filter the list of String containing tag
    * @param lines
    * @param tag
    * @return A new list of strings, all containing the tag.
    */
   public String[] filterLines(String[] lines, String tag) {
      int count = 0;
      for (String line : lines) {
         if (line.contains(tag))
            count++;
      }

      String[] filteredLines = new String[count];
      int index=0;
      for (String line : lines) {
         if (line.contains(tag))
         {
            filteredLines[index] = line;
            index++;
         }
      }

      return filteredLines;
   }

   /**
    * Reads the lundstrom output with option -s and returns the estimated remaining time from currentStage
    * @param lundstromOutput
    * @param currentStage
    * @return estimated remaining time
    */
   public long getRemainingTime(String lundstromOutput, String currentStage) {
      long totalTime = getTotalExecutionTime(lundstromOutput);
      long stageWaitTime = getStageWaitTime(lundstromOutput, currentStage);


      return totalTime - stageWaitTime;
   }

   /**
    * Retrieve all the stages ids from the lundstrom output
    * @param lundstromOutput the output of lundstrom run with -s option
    * @return
    */
  //  public String[] getAllStages(String lundstromOutput) {
  //     JSONParser parser = new JSONParser();
  //     JSONObject json = (JSONObject) parser.parse(lundstromOutput);
   //
  //     JSONArray stgs = (JSONArray) json.get("stages");
  //     int num_stages = stgs.length;
   //
  //     String[] stages = new String[num_stages];
  //     for (int i = 0; i < num_stages; i++)
  //       stages[i] = stgs.get(i).get("id");
   //
  //     return stages;
  //  }

   /**
    * From the full lundstromOutput extracts the total time (3rd row, 3rd column)
    * @param lundstromOutput
    * @return
    */
    public long getTotalExecutionTime(String lundstromOutput) {
      /** @todo improve json parsing to single point */
      try
      {
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(lundstromOutput);
        return (long)Math.floor(Double.valueOf((String)json.get("real")));
      }
      catch (ParseException pe)
      {
        System.out.println(pe);
        return 0;
      }
    }
  //  public long getTotalExecutionTime(String lundstromOutput) {
  //     return (long)Math.floor(Double.valueOf(extract(lundstromOutput.split("\n")[0], 3, "\t")));
  //  }

   /**
    * From the full lundstromOutput extracts the stage wait time (2nd row, 4th column of the stage-relative rows)
    * @param lundstromOutput
    * @param stage
    * @return
    */
    public long getStageWaitTime(String lundstromOutput, String stage) {
       try
       {
         JSONParser parser = new JSONParser();
         JSONObject json = (JSONObject) parser.parse(lundstromOutput);
         JSONObject st;
         JSONArray stgs = (JSONArray) json.get("stages");
         int num_stages = stgs.size();
         long val = 0;

         String[] stages = new String[num_stages];
         for (int i = 0; i < num_stages; i++) {
           st = (JSONObject) stgs.get(i);
           if (stage.equals(st.get("id")))
             val = (long)Math.floor(Double.valueOf((String)st.get("time")));
         }

         return val;
       }
       catch (ParseException pe)
       {
         System.out.println(pe);
         return 0;
       }
    }
  //  public long getStageWaitTime(String lundstromOutput, String stage) {
  //     String[] stageOutput = filterLines(lundstromOutput.split("\n"), stage);
  //     return (long)Math.floor(Double.valueOf(extract(stageOutput[1], 4, "\t")));
  //  }

   public int extract(String string, int position)
   {
      String mysubstring = "", savestring = "";


      for (int i = 1; i <= position; i++)
      {
         mysubstring = string.substring(0, string.indexOf("_"));
         savestring = mysubstring;
         string = string.substring(mysubstring.length()+1, string.length());
      }

      return Integer.valueOf(mysubstring);

      // return Integer.valueOf(extract(string, position, "_"));
   }

   /**
    * Returns the n-th substring of s according to the separator. position is 1-based
    * @param s
    * @param position
    * @param separator
    * @return
    */
   public String extract(String s, int position, String separator) {
      String[] elems = s.split(separator);

      return elems[position - 1];
   }

   public String uploadCsvToTable(String uploadedFileLocation, String filename)
   {
      String cmd = readVar("OPTIMIZE_HOME")+"/optimize "+uploadedFileLocation + " 100 " + filename;
      return _run(cmd);
   }




   public String readVar(String property)
   {
      String line, path="";
      int index;
      try {
         InputStream fis = new FileInputStream(System.getenv("HOME").concat("/.ws_properties"));
         InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
         BufferedReader br = new BufferedReader(isr);

         while ((line = br.readLine()) != null) {
            if (line.contains(property))
            {
               index = line.indexOf('=');
               path=line.substring(index+1, line.length());
               break;
            }
         }
         br.close();
      }
      catch (IOException e)
      {
         e.printStackTrace();
      }
      return(path);
   }

   // BuildLUA(resultsPath, newNodes, newCores, ramGB, datasetSize, appId)
   public String BuildLUA(String resultsPath, String newNodes, String newCores, String ramGB, String datasetSize, String appId)
   {
      return " -n "+ newNodes + " -c "+ newCores + " -r " + ramGB + " -d " + datasetSize + " -q " + appId + " -p spark";
   }
  //  public String BuildLUA(String resultsPath, String newNodes, String newCores, String ramGB, String datasetSize, String appId)
  //  {
  //     String path = resultsPath.concat("/").concat(newNodes).concat("_").concat(newCores).concat("_").concat(ramGB).concat("_").concat(datasetSize).concat("/").concat(appId).concat("/logs/");
   //
  //     String luaFileName = getFirstFile(getFirstFolder(path));
   //
  //     return(luaFileName);
  //  }

   public String BuildLUAWithoutMethod(String resultsPath, final String nNodes, final String nCores, final String datasetSize, String appId) {
      // List all files in results path directory
      File [] directories = new File(resultsPath).listFiles(new FileFilter() {
         @Override
         public boolean accept(File file)
         {
            return file.isDirectory() && file.getName().startsWith(nNodes + "_" + nCores) && file.getName().endsWith(datasetSize);
         }
      });
      if (directories.length > 0) {
        String ramGB = directories[0].getPath().replace(nNodes+"_"+nCores+"_", "").replace("_"+datasetSize, "");
        return BuildLUA(resultsPath, nNodes, nCores, ramGB, datasetSize, appId);
        //  String path = directories[0].getPath().concat("/").concat(query).concat("/logs/");
        //  String luaFileName = getFirstFile(getFirstFolder(path));
        //  return luaFileName;
      }
      return null;
   }



   public String getFirstFolder(String path)
   {
      String folderName = path;
      try
      {
         File folder = new File(path);
         File[] listOfFiles = folder.listFiles();
         int i;

         Arrays.sort(listOfFiles);
         for (i = 0; i < listOfFiles.length; i++)
            if (listOfFiles[i].isDirectory()) break;
         folderName = listOfFiles[i].getAbsolutePath();
      }
      catch(Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

      return folderName.concat("/");
   }

   public String getFirstFile(String path)
   {
      // Initialize to worst case: something is wrong with the filename or the path
      String filename = "ERROR: could not find file";
      try
      {
         File folder = new File(path);
         File[] listOfFiles = folder.listFiles();
         int i;

         for (i = 0; i < listOfFiles.length; i++)
            if (
                  (listOfFiles[i].isFile()) &&
                  (listOfFiles[i].getName().endsWith(".lua"))
               ) break;
         filename = listOfFiles[i].getAbsolutePath();
      }
      catch(Exception e)
      {
         e.printStackTrace();

      }

      return filename;
   }

   public String _run(String cmd) {
      try
      {
         final ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
         pb.redirectErrorStream();
         final Process process = pb.start();
         final InputStream in = process.getInputStream();
         final byte[] bytes = new byte[1024];
         final ByteArrayOutputStream baos = new ByteArrayOutputStream();


         for (int len; (len = in.read(bytes)) > 0;)
            baos.write(bytes, 0, len);
         process.waitFor();
         return baos.toString();

      }
      catch(IOException e)
      {
         e.printStackTrace();
      }
      catch(InterruptedException e)
      {
         e.printStackTrace();
      }
      return("Error: could not execute command "+cmd);
   }

   // save uploaded file to new location
   public void writeToFile(InputStream uploadedInputStream,
         String uploadedFileLocation) {

      try {
         OutputStream out = new FileOutputStream(new File(
                  uploadedFileLocation));
         int read = 0;
         byte[] bytes = new byte[1024];

         out = new FileOutputStream(new File(uploadedFileLocation));
         while ((read = uploadedInputStream.read(bytes)) != -1) {
            out.write(bytes, 0, read);
         }
         out.flush();
         out.close();
      } catch (IOException e) {

         e.printStackTrace();
      }

   }

   /**
    * Search in table RUNNING_APPLICATION_TABLE and get corresponding app_id
    * @param appSessId
    * @param dbName
    * @param connection
    * @return
    * @throws SQLException
    */
   public String retrieveAppId(String appSessId, String dbName, Connection connection) throws SQLException {
      String query_stmt = "SELECT application_id FROM " + dbName + ".RUNNING_APPLICATION_TABLE WHERE application_session_id = '" + appSessId + "'";

      ResultSet results = query(dbName, connection, query_stmt);
      if (results != null && results.next()) {
         return results.getString("application_id");
      }
      throw new SQLException("Could not retrieve application ID from database");
   }

   /**
    * Search in table RUNNING_APPLICATION_TABLE for submission time
    * @param appSessId
    * @param dbName
    * @param connection
    * @return
    * @throws SQLException
    */
   public Timestamp retrieveSubmissionTime(String appSessId, String dbName, Connection connection) throws SQLException {
      String get_submission_time_query = "SELECT submission_time FROM " + dbName + ".RUNNING_APPLICATION_TABLE WHERE application_session_id = '"+ appSessId + "'";

      ResultSet results = query(dbName, connection, get_submission_time_query);
      if (results != null && results.next()) {
         return results.getTimestamp("submission_time");
      }
      throw new SQLException("Could not retrieve submission time from database");
   }

   /**
    * Finds the best suitable choice for nNodes and nCores if we don't have any directly associated data
    * @param resultsPath
    * @param nNodes
    * @param nCores
    * @param memory
    * @param dataset
    * @param appId
    * @return A (possibly null) string of type "<nNodes> <nCores>". If the correct folder is found, then the two values will correspond to the parameters. A null string is returned if the results folder has not been found.
    */
   public String findBestMatch(String resultsPath, String nNodes, String nCores, String memory, String dataset, String appId) {
      String path = resultsPath + "/" + nNodes + "_" + nCores + "_" + memory + "_" + dataset +"/" + appId;
      File f = new File(path);
      if (!f.exists())
      {
         // Get the list of all the folders
         File [] directories = new File(resultsPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file)
            {
               return file.isDirectory();
            }
         });

         if (directories == null)
         {
            System.out.println("Fatal error: no sub-directories have been found in " + resultsPath);
            return null;
         }
         String reply = bestMatch(directories, nNodes, nCores, memory, dataset, appId);

         return reply;
      }
      else
         return nNodes + " " + nCores + " ";
   }

   /**
    * Search in the lookup table for precomputed values and returns the result
    * @param connection
    * @param dbName
    * @param appId
    * @param nCores
    * @param stage
    * @param dataset
    * @return A ResultSet containing 'stage_end_time' and 'remaining_time' if results are precomputed. Null otherwise
    * @throws SQLException
    */

   public ResultSet lookupLundstromStageEndTime(Connection connection, String dbName, String appId, String nCores, String stage, String dataset) throws SQLException {
      String query = "SELECT val FROM " + dbName + ".PREDICTOR_CACHE_TABLE WHERE is_residual = FALSE AND application_id = '" + appId + "' AND num_cores = " + nCores +
    		  " AND stage='" + stage + "' AND dataset_size=" + dataset;

      ResultSet results = query(dbName, connection, query);
      return results;
   }

   public ResultSet lookupLundstromStageRemainingTime(Connection connection, String dbName, String appId, String nCores, String stage, String dataset) throws SQLException {
      String query = "SELECT val FROM " + dbName + ".PREDICTOR_CACHE_TABLE WHERE is_residual = TRUE AND application_id = '" + appId + "' AND num_cores = " + nCores + " AND stage='" + stage + "' AND dataset_size=" + dataset;

      ResultSet results = query(dbName, connection, query);
      return results;
   }

   /**
    * Saves the results in the lookup table
    * @param connection
    * @param dbName
    * @param appId
    * @param nCores
    * @param stage
    * @param dataset
    * @param stage_end_time
    * @param remainingTime
    * @throws SQLException
    */
   public void saveLookupLundstromStages(Connection connection, String dbName, String appId, String nCores, String stage, String dataset, long stage_end_time, long remainingTime) throws SQLException {
      String save_stage_end = "REPLACE INTO " + dbName + ".PREDICTOR_CACHE_TABLE(application_id, num_cores, stage, dataset_size, val, is_residual) VALUES ('" + appId + "', " + nCores + ", '" + stage + "', " + dataset + ", " + stage_end_time + ", FALSE)";
      String save_remaining_time = "REPLACE INTO " + dbName + ".PREDICTOR_CACHE_TABLE(application_id, num_cores, stage, dataset_size, val, is_residual) VALUES ('" + appId + "', " + nCores + ", '" + stage + "', " + dataset + ", " + remainingTime + ", TRUE)";

      insert(dbName, connection, save_stage_end);
      insert(dbName, connection, save_remaining_time);
      connection.commit();
   }



    /**
     * Retrieve all the stages ids from the lundstrom output
     * @param lundstromOutput the output of lundstrom run with -s option
     * @return
     */
    public String[] getAllStages(String lundstromOutput) {
      String[] stages = new String[0];
      try {
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(lundstromOutput);
        JSONObject st;

        JSONArray stgs = (JSONArray) json.get("stages");
        int num_stages = stgs.size();

        stages = new String[num_stages];
        for (int i = 0; i < num_stages; i++) {
          st = (JSONObject) stgs.get(i);
          stages[i] = (String) st.get("id");
        }

        return stages;
      }
      catch (ParseException pe)
      {
        System.out.println(pe);
        return stages;
      }
    }
  //  public String[] getAllStages(String lundstromOutput) {
  //     String[] lines = lundstromOutput.split("\n");
  //     int num_stages = (lines.length / 4) - 1;
   //
  //     String[] stages = new String[num_stages];
  //     for (int i = 0; i < num_stages; i++)
  //        stages[i] = extract(lines[i*4 + 4], 9, "\t");
   //
  //     return stages;
  //  }
}
