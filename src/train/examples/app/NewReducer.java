package train.examples.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.JSONException;

public class NewReducer extends Reducer<Text, Text, Text, Text> {

	private String[] ops_distinct;
	private String[] ops_other;
	private Text reduceOutValue = new Text();
	private MultipleOutputs<Text, Text> mos;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
		try {
			ExtraWork.setBasePath(context.getConfiguration().get("basePath"));
			ExtraWork.setJobID(context.getConfiguration().get("jobID"));
			ExtraWork.parseJson();
			ops_distinct = ExtraWork.getOps_distinct();
			ops_other = ExtraWork.getOps_other();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		//write the end of each json file generated in map
		File jsonFile = new File("/home/kong/temp/" + context.getConfiguration().get("jobID") + "/json/" + key.toString().substring(0, key.toString().length() - 1));
		BufferedWriter jsonWriter = new BufferedWriter(new FileWriter(jsonFile, true));
		jsonWriter.append("\n]\n}");
		jsonWriter.close();
		
		//generate the reduce output value		
		HashSet<String> distinctSet = new HashSet<String>();
		int[] sum = new int[ops_other.length];//sum[i] == 0
		if(ops_distinct.length > 0) {
			for(Text val : values) {
				distinctSet.add(val.toString().split(";")[0]);
				for(int i = 0; i < sum.length; i++) {
					sum[i] += Integer.parseInt(val.toString().split(";")[1 + i]);
				}
				
			}
		} else {
			for(Text val : values) {
				for(int i = 0; i < sum.length; i++) {
					sum[i] += Integer.parseInt(val.toString().split(";")[i]);	
				}
				
			}
		}
		
		
		StringBuilder tempVal = new StringBuilder();
		if(ops_distinct.length > 0)
			tempVal.append(distinctSet.size() + ",");
		for(int i = 0; i < sum.length; i++) {
			tempVal.append(sum[i]);
			if(i != sum.length - 1)
				tempVal.append(',');
		}
		
		reduceOutValue.set(tempVal.toString());
		//context.write(key, reduceOutValue);
		//write the reduce output to different key-related files
		mos.write(key.toString().substring(key.toString().length() - 1), key, reduceOutValue);
			
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		super.cleanup(context);
	}

}
