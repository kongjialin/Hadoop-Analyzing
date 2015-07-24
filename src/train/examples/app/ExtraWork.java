package train.examples.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class ExtraWork {
	
	/**
	 * get the titles of each column(column names) of the data in the input file
	 * @return the array of Strings storing each column name as an element
	 * @throws IOException 
	 */
	public static String[] getColumnTitles() throws IOException {
		
		Properties prop = new Properties();
		String configFile = "/config/config.properties";
		prop.load(ExtraWork.class.getResourceAsStream(configFile));
		String fileName = prop.getProperty("base_dir") + prop.getProperty("colTitles");
		File file = new File(fileName);
		BufferedReader reader = null;
		String columnTitles = new String();
		try {
			reader = new BufferedReader(new FileReader(file));
			columnTitles = reader.readLine();	
		} finally {
			if(reader != null)
				reader.close();
		}
		
		String[] splittedColumns = columnTitles.split(",");
		return splittedColumns;
		
	}
	
	/**
	 * read the json file that specifies the row/column labels and the operations of specified data,
	 * and parse out the Strings
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void parseJson() throws IOException, JSONException {
		
		String fileName = basePath + jobID + "Meta";
		File file = new File(fileName);
		//File file = FileSystem.getLocal(new Configuration()).pathToFile(new Path(basePath + jobID + "Meta"));
		Reader reader = null;
		StringBuilder sb = new StringBuilder();
		try {
			reader = new InputStreamReader(new FileInputStream(file));
			int tempchar;
			while((tempchar = reader.read()) != -1) {
				sb.append((char)tempchar);
			}
		} finally {
			
			if(reader != null) 
				reader.close();
		}
		
		JSONObject jo = new JSONObject(sb.toString());
		
		//parse the row labels
		JSONArray rowArray = jo.getJSONObject("dimension").getJSONArray("rows");
		rowLabels = new String[rowArray.length()];
		for(int ir = 0; ir < rowArray.length(); ir++) {
			rowLabels[ir] = new String(rowArray.getString(ir));
		}
		
		//parse the column labels
		JSONArray colArray = jo.getJSONObject("dimension").getJSONArray("columns");
		colLabels = new String[colArray.length()];
		for(int ic = 0; ic < colArray.length(); ic++) {
			colLabels[ic] = new String(colArray.getString(ic));
		}
		
		//parse the operations and values
		JSONArray opsArray = jo.getJSONObject("measure").getJSONArray("operations");
		JSONArray valArray = jo.getJSONObject("measure").getJSONArray("values");
		StringBuilder ops_distinct = new StringBuilder();
		StringBuilder ops_other = new StringBuilder();
		StringBuilder vals_distinct = new StringBuilder();
		StringBuilder vals_other = new StringBuilder();
		for(int io = 0; io < opsArray.length(); io++) {
			if(opsArray.getString(io).equals("count distinct")) {
				ops_distinct.append(opsArray.getString(io)).append(",");
				vals_distinct.append(valArray.getString(io)).append(",");
			} else {
				ops_other.append(opsArray.getString(io)).append(",");
				vals_other.append(valArray.getString(io)).append(",");
			}
		}
		
		if(ops_other.length() > 0) {
			opsOther = ops_other.toString().split(",");
			valsOther = vals_other.toString().split(",");
			ops = opsOther;
			vals = valsOther;
		}
		if(ops_distinct.length() > 0) {
			opsDistinct = ops_distinct.toString().split(",");
			valsDistinct = vals_distinct.toString().split(",");
			ops = new String("count distinct,").concat(ops_other.toString()).split(",");
			StringBuilder val = new StringBuilder();
			for(int i = 0; i < valsDistinct.length; i++) {
				val.append(valsDistinct[i]);
				if(i != valsDistinct.length - 1)
					val.append('&');
			}
			vals = val.append(',' + vals_other.toString()).toString().split(",");
		}	

		//parse the mapreduce job name
		jobName = jo.getString("jobName");
	}
	
	/**
	 * public API to get the row/column labels and values/operations
	 */
	public static String[] getRows() {
		return rowLabels;
	}
	public static String[] getCols() {
		return colLabels;
	}
	public static String[] getVals_distinct() {
		return valsDistinct;
	}
	public static String[] getVals_other() {
		return valsOther;
	}
	public static String[] getOps_distinct() {
		return opsDistinct;
	}
	public static String[] getOps_other() {
		return valsOther;
	}
	
	/**
	 * make a few times of needed comparison to generate the location index of the data of each line of the input file;
	 * will be called only once in the setup() method of the Mapper class to serve the map() method
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static void setSubKeyValIndex() throws IOException, JSONException {
		
		parseJson();
		String[] columnTitles = getColumnTitles();
		
		//set subKeyIndex
		subKeyIndex = new int[rowLabels.length + colLabels.length];
		int i = 0;
		for(; i < rowLabels.length; i++) {
			int j = 0;
			for(; j < columnTitles.length; j++) {
				if(rowLabels[i].equals(columnTitles[j]))
					break;
			}
			subKeyIndex[i] = j;
		}
		for(int ic = 0; ic < colLabels.length; ic++) {
			int j = 0;
			for(; j < columnTitles.length; j++) {
				if(colLabels[ic].equals(columnTitles[j]))
					break;
			}
			subKeyIndex[i+ic] = j;
		}
		
		//set subValIndex_distinct and subValIndex_other
		subValIndex_distinct = new int[opsDistinct.length];
		subValIndex_other = new int[opsOther.length];
		for(int io = 0; io < opsDistinct.length; io++) {
			int j = 0;
			for(; j < columnTitles.length; j++) {
				if(valsDistinct[io].equals(columnTitles[j]))
					break;
			}
			subValIndex_distinct[io] = j;
		}
		for(int io = 0; io < opsOther.length; io++) {
			if(opsOther[io].equals("count"))
				subValIndex_other[io] = -1;
			else {
				int j = 0;
				for(; j < columnTitles.length;j++) {
					if(valsOther[io].equals(columnTitles[j]))
						break;
				}
				subValIndex_other[io] = j;
			}
		}
		
	}
	
	/**
	 * public API to get the subKey/ValIndex
	 */
	public static int[] getSubKeyIndex() {
		return subKeyIndex;
	}
	public static int[] getSubValIndex_distinct() {
		return subValIndex_distinct;
	}
	public static int[] getSubValIndex_other() {
		return subValIndex_other;
	}
	
	/**
	 * move the reduce output files from hdfs to local file system
	 * not used currently
	 * @throws IOException 
	 */
	public static void copyToLocal(Configuration conf) throws IOException {

		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
		
		FileSystem fs = FileSystem.get(conf);
		fs.copyToLocalFile(false, new Path("/output/part-r-00000"), new Path("/home/kong/output/part-r-00000"), true);
		fs.close();
	}
	
	/**
	 * delete the output path of mapreduce on hdfs
	 */
	public static void clearHdfs(Configuration conf) throws IOException {
			
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(hdfsPath), true);
		fs.close();
	}
	
	/**
	 * generate file names of different reduce output files
	 */
	public static String[] generateFileNames() {
		
		String[] fileNames;	
		if(rowLabels.length > 0) {
			fileNames = new String[rowLabels.length];
			for(int i = 0; i < rowLabels.length; i++) {
				fileNames[i] = Integer.toString(i + 1);
			}
		} else {
			fileNames = new String[1];
			fileNames[0] = Integer.toString(0);
		}
		return fileNames;
	}
	
	/**
	 * generate the json file for web display
	 * make sure the parseJson() method has been called before this method
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static void generateJson(Configuration conf) throws JSONException, IOException {
		
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(hdfsPath + "part-r-00000");
		
		JSONObject json = new JSONObject("{\"data\": [],\"metadata\": {}}");
		JSONArray dataArray = json.getJSONArray("data");
		JSONObject metaObject = json.getJSONObject("metadata");
		metaObject.put("dimension", new JSONObject().put("rows", new JSONArray(rowLabels)).put("columns", new JSONArray(colLabels)));
		metaObject.put("measure", new JSONObject().put("operations", new JSONArray(ops)).put("values", new JSONArray(vals)));

		List<HashSet<String>> colSet = new ArrayList<HashSet<String>>();
		for(int i = 0; i < colLabels.length; i++) 
			colSet.add(new HashSet<String>());
		
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new InputStreamReader(fs.open(output)));

			while((line = reader.readLine()) != null) {
				String[] keys = line.split("\t")[0].split(",");
				String[] values = line.split("\t")[1].split(",");
				
				//collect the strings of column labels(used in forming the "metadata" in json)
				if(keys.length == rowLabels.length + colLabels.length + 1) {
					for(int i = 0; i < colLabels.length; i++) {
						colSet.get(i).add(keys[rowLabels.length + i]);
					}
				}
				
				//generate the JSONArray corresponding to "data" in json
				if(dataArray.length() != 0) {
					int ir = 0;
					for(; ir < rowLabels.length; ir++) {
						if(!keys[ir].equals(dataArray.getJSONObject(dataArray.length() - 1).getString(rowLabels[ir])))
							break;
					}
					if(ir == rowLabels.length) {
						insertPair(keys, values, dataArray);
					} else {
						addNewObject(keys, values, dataArray);
					}
				} else {
					addNewObject(keys, values, dataArray);
				}	
			}
			
		} finally {
			if(reader != null)
				reader.close();
			fs.close();
		}
		
		for(int ic = 0; ic < colSet.size(); ic++) {
			metaObject.put(colLabels[ic], colSet.get(ic));
		}
		
		json.put("data",dataArray);
		json.put("metadata", metaObject);
		
		File writeFile = new File(basePath + jobID);
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(writeFile));
			writer.write(json.toString(4));
		} finally {
			if(writer != null)
				writer.close();
		}
		
	}
	
	/**
	 * insert new name/value pair(s) to a JSONObject, which means filling the content of one line in the pivot table
	 * called by generateJson()
	 * @throws JSONException
	 */
	private static void insertPair(String[] keys, String[] values, JSONArray dataArray) throws JSONException {
		
		String tempString = new String();
		for(int ic = rowLabels.length; ic < keys.length - 1; ic++) {
			tempString = tempString.concat(keys[ic]).concat("_");
		}
		for(int iv = 0; iv < vals.length; iv++) {
			dataArray.getJSONObject(dataArray.length() - 1).put(tempString.concat(vals[iv]), values[iv]);
		}
	}
	
	/**
	 * add a new JSONObject to the JSONArray, which means adding a new line of data in the pivot table
	 * called by generateJson()
	 * @throws JSONException
	 */
	private static void addNewObject(String[] keys, String[] values, JSONArray dataArray) throws JSONException {
		
		JSONObject tempObj = new JSONObject();
		int ir = 0;
		for(; ir < rowLabels.length; ir++) {
			tempObj.put(rowLabels[ir], keys[ir]);
		}
		String tempString = new String();
		for(int ic = ir; ic < keys.length - 1; ic++) {
			tempString = tempString.concat(keys[ic]).concat("_");
		}
		for(int iv = 0; iv < vals.length; iv++) {
			tempObj.put(tempString.concat(vals[iv]), values[iv]);
		}
		dataArray.put(tempObj);
	}
	
	/**
	 * generate another format of json for web input, which is in a nested format like a tree
	 * @param conf
	 * @throws IOException
	 * @throws JSONException
	 */
	public static void generateAnotherJson(Configuration conf) throws IOException, JSONException {
		
		JSONObject json = new JSONObject("{\"children\": [],\"metadata\": {}}");
		JSONArray childrenArr = json.getJSONArray("children");
		JSONObject metaObject = json.getJSONObject("metadata");
		
		metaObject.put("dimension", new JSONObject().put("rows", new JSONArray(rowLabels)).put("columns", new JSONArray(colLabels)));
		metaObject.put("measure", new JSONObject().put("operations", new JSONArray(ops)).put("values", new JSONArray(vals)));

		List<HashSet<String>> colSet = new ArrayList<HashSet<String>>();
		for(int i = 0; i < colLabels.length; i++) 
			colSet.add(new HashSet<String>());
		
		FileSystem fs = FileSystem.get(conf);
		Path path = null;
		BufferedReader reader = null;
		String line = null;
		String[] keys = null;
		String[] values = null;
		if(rowLabels.length > 0) {
			path = new Path(hdfsPath + "1-r-00000");
			reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			while((line = reader.readLine()) != null) {
				keys = line.split("\t")[0].split(",");
				values = line.split("\t")[1].split(",");
				putLine(0, keys, values, childrenArr);
				
				//generate the column names for web
				if(keys.length == colLabels.length + 2) {
					for(int ic = 0; ic < colLabels.length; ic++) {
						colSet.get(ic).add(keys[ic + 1]);
					}
				}
							
			}
			reader.close();
			
			for(int i = 1; i < rowLabels.length; i++) {
				int[] pos = new int[i];
				path = new Path(hdfsPath + Integer.toString(i + 1) + "-r-00000");
				reader = new BufferedReader(new InputStreamReader(fs.open(path)));
				while((line = reader.readLine()) != null) {
					keys = line.split("\t")[0].split(",");
					values = line.split("\t")[1].split(",");
					JSONArray subChildrenArr = childrenArr;
					
					for(int j = 0; j < i; j++) {
						if(!keys[j].equals(subChildrenArr.getJSONObject(pos[j]).getString("ROWNAME"))) {
							pos[j]++;
							for(int k = j + 1; k < i; k++)
								pos[k] = 0;
						}		
						subChildrenArr = subChildrenArr.getJSONObject(pos[j]).getJSONArray("children");
					}
					putLine(i, keys, values, subChildrenArr);
				}
				reader.close();
			}
			
		} else {
			JSONObject child = new JSONObject("{\"ROWNAME\":\"\", \"leaf\":true}");
			childrenArr.put(child);
			path = new Path(hdfsPath + "0-r-00000");
			reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			while((line = reader.readLine()) != null) {
				keys = line.split("\t")[0].split(",");
				values = line.split("\t")[1].split(",");
				insertPairs(-1, keys, values, childrenArr);
				
				if(keys.length == colLabels.length + 1) {
					for(int ic = 0; ic < colLabels.length; ic++) {
						colSet.get(ic).add(keys[ic]);
					}
				}		
			}
			reader.close();
		}
		
		for(int ic = 0; ic < colLabels.length; ic++) {
			metaObject.put(colLabels[ic], colSet.get(ic));
		}
		
		json.put("children", childrenArr);
		json.put("metadata", metaObject);
		
		File writeFile = new File(basePath + jobID);
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(writeFile));
			writer.write(json.toString(4));
		} finally {
			if(writer != null)
				writer.close();
		}
	}
	
	/**
	 * called by generateAnotherJson(), used to put each line of the file into json
	 */
	private static void putLine(int i, String[] keys, String[] values, JSONArray childrenArr) throws JSONException {
		
		if(childrenArr.length() > 0) {
			if(keys[i].equals(childrenArr.getJSONObject(childrenArr.length() - 1).getString("ROWNAME"))) {
				insertPairs(i, keys, values, childrenArr);
			} else {
				addNewChild(i, keys, values, childrenArr);
			}	
			
		} else {
			addNewChild(i, keys, values, childrenArr);
		}
	}
	
	/**
	 * called by putLine(), used to insert name/value pairs in one JSONObject
	 */
	private static void insertPairs(int i, String[] keys, String[] values, JSONArray childrenArr) throws JSONException {
		
		String temp = new String();
		for(int ic = i + 1; ic < keys.length - 1; ic++) {
			temp = temp.concat(keys[ic] + '_');
		}
		for(int iv = 0; iv < values.length; iv++) {
			childrenArr.getJSONObject(childrenArr.length() - 1).put(temp.concat(vals[iv]), values[iv]);
		}
	}
	
	/**
	 * called by putLine(), used to add a new JSONObject in an JSONArray
	 */
	private static void addNewChild(int i, String[] keys, String[] values, JSONArray childrenArr) throws JSONException {
		
		JSONObject childObj = new JSONObject();
		childObj.put("ROWNAME", keys[i]);
		String temp = new String();
		for(int ic = i + 1; ic < keys.length - 1; ic++) {
			temp = temp.concat(keys[i] + '_');
		}
		for(int iv = 0; iv < values.length; iv++) {
			childObj.put(temp.concat(vals[iv]), values[iv]);
		}
		if(i == rowLabels.length - 1) {
			childObj.put("leaf", true);
		} else {
			childObj.put("children", new JSONArray());
		}
		
		childrenArr.put(childObj);
	}
	
	/**
	 * return the mapreduce job name
	 */
	public static String getJobName() {
		return jobName;
	}
	
	/**
	 * public APIs to set the base path, job ID and hdfs path based on the command line arguments
	 */
	public static void setBasePath(String path) {
		basePath = path;
	}
	public static void setJobID(String id) {
		jobID = id;
	}
	public static void setHdfsPath(String path) {
		hdfsPath = path;
	}
	
	/**
	 * arrays used to store the elements of the pivot table
	 */
	private static String[] rowLabels;
	private static String[] colLabels;
	private static String[] opsDistinct = new String[0];
	private static String[] opsOther = new String[0];
	private static String[] ops;
	private static String[] valsDistinct = new String[0];
	private static String[] valsOther = new String[0];
	private static String[] vals;
	
	/**
	 * arrays used to locate the data corresponding to the specified column titles
	 * they are helpful in improving the efficiency because:
	 * In the method map(), the data corresponding to the specified column titles are extracted from a line, as the 
	 * subkeys to form the map output key. 
	 * Locating and extracting the data need several times of comparison in a loop, and each line in the input file 
	 * will trigger the map() for once. 
	 * In order to avoid thousands of times of comparison, we make only a few times of needed comparison when the 
	 * Mapper set up, before the execution of map(), and store the location index of the data in the array. This work
	 * is done in the method setSubKeyValIndex(). Then each time the map() is called, it only needs to use the location
	 * index directly from the array and doesn't need comparison any more.
	 */
	private static int[] subKeyIndex;
	private static int[] subValIndex_distinct;
	private static int[] subValIndex_other;
	
	private static String jobName;
	private static String basePath;
	private static String jobID;
	private static String hdfsPath;
}

