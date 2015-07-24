package train.examples.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class NewMapper extends Mapper<Object, Text, Text, Text> {

	private Text mapOutKey = new Text();
	private Text mapOutValue = new Text();
	private int[] subKeyIndex;
	private int[] subValIndex_distinct;
	private int[] subValIndex_other;
	private String[] columnTitles;
	private String columns;
	private String[] rowLabels;
	private String[] colLabels;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		columnTitles = ExtraWork.getColumnTitles();
		StringBuilder sb = new StringBuilder();
		for(int ic = 0; ic < columnTitles.length; ic++) {
			sb.append(columnTitles[ic]);
			if(ic != columnTitles.length - 1)
				sb.append(',');
		}
		columns = sb.toString();
		try {
			ExtraWork.setBasePath(context.getConfiguration().get("basePath"));
			ExtraWork.setJobID(context.getConfiguration().get("jobID"));
			ExtraWork.setSubKeyValIndex();
			subKeyIndex = ExtraWork.getSubKeyIndex();
			subValIndex_distinct = ExtraWork.getSubValIndex_distinct();
			subValIndex_other = ExtraWork.getSubValIndex_other();
			rowLabels = ExtraWork.getRows();
			colLabels = ExtraWork.getCols();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		//in case of the null value at the end of the line being ignored, concatenate ",END" to the end of the line
		//e.g:  A,B,C ---3 strings; while A,B, ---2 strings
		//so--> A,B,C,END;A,B,,END
		String valueToString = value.toString().concat(",END");  
		String[] splittedValue = valueToString.split(",");
		
		JSONObject jo = new JSONObject();
		try {
			for(int ic = 0; ic < columnTitles.length; ic++) 
				jo.put(columnTitles[ic], splittedValue[ic]);
		} catch (JSONException ex) {
			ex.printStackTrace();
		}
		
		//generate the map output value
		StringBuilder tempValue = new StringBuilder();
		
		for(int i = 0; i < subValIndex_distinct.length; i++) {
			tempValue.append(splittedValue[subValIndex_distinct[i]]);
			if(i == subValIndex_distinct.length - 1)
				tempValue.append(';');
			else 
				tempValue.append(',');
		}
		for(int i = 0; i < subValIndex_other.length; i++) {
			if(subValIndex_other[i] == -1) 
				tempValue.append("1;");
			else {
				if(splittedValue[subValIndex_other[i]].equals("") || !splittedValue[subValIndex_other[i]].matches("\\d+"))
					splittedValue[subValIndex_other[i]] = "0";
				tempValue.append(splittedValue[subValIndex_other[i]]).append(';');
			}
		}
		
		mapOutValue.set(tempValue.toString());
		
		//generate the map output key and write each key/value pair 
		if(rowLabels.length > 0) {
			StringBuilder row = new StringBuilder();
			for(int ir = 0; ir < rowLabels.length; ir++) {
				row.append(splittedValue[subKeyIndex[ir]] + ',');
				String temp = row.toString();
				for(int ic = 0; ic <= colLabels.length; ic++) {
					if(ic != 0)
						temp = temp.concat(splittedValue[subKeyIndex[rowLabels.length + ic - 1]] + ',');
					toFile(temp.concat("END"), value, context, jo);
					mapOutKey.set(temp.concat("END" + Integer.toString(ir + 1)));
					context.write(mapOutKey, mapOutValue);
				}
			}
		} else {
			String temp = new String();
			for(int ic = 0; ic <= colLabels.length; ic++) {
				if(ic != 0)
					temp = temp.concat(splittedValue[subKeyIndex[ic - 1]] + ',');
				toFile(temp.concat("END"), value, context, jo);
				mapOutKey.set(temp.concat("END0"));
				context.write(mapOutKey, mapOutValue);
			}
		}
		
	}
	
	private void toFile(String tempKey, Text value, Context context, JSONObject jo) throws IOException {
		
		//write the detail of a line into a key-related csv file
		File csvFile = new File("/home/kong/temp/" + context.getConfiguration().get("jobID") + "/csv/" + tempKey + ".csv");
		if(!csvFile.exists()) {
			BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFile));
			csvWriter.write(columns);
			csvWriter.write('\n');
			csvWriter.write(value.toString());
			csvWriter.close();
		} else {
			BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFile, true));
			csvWriter.append("\n");
			csvWriter.append(value.toString());
			csvWriter.close();
		}
		
		//write the json-format detail into a key-related json file
		try {
			File jsonFile = new File("/home/kong/temp/" + context.getConfiguration().get("jobID") + "/json/" + tempKey);			
			if(!jsonFile.exists()) {
				BufferedWriter jsonWriter = new BufferedWriter(new FileWriter(jsonFile));
				jsonWriter.write("{\"item\":[\n");
				jsonWriter.write(jo.toString(4));
				jsonWriter.close();
			} else {
				BufferedWriter jsonWriter = new BufferedWriter(new FileWriter(jsonFile, true));
				jsonWriter.append(",\n");
				jsonWriter.append(jo.toString(4));
				jsonWriter.close();
			}
		} catch(JSONException ex) {
			ex.printStackTrace();
		}
	}
	
}
