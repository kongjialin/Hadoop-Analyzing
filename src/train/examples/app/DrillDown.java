package train.examples.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class DrillDown extends Configured implements Tool {

	public static class DrillDownMapper extends Mapper<Object, Text, Text, NullWritable> {
	
		private String[] searchValue;
		private int[] searchIndex;
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			String[] columnTitles = ExtraWork.getColumnTitles();
			searchIndex = new int[context.getConfiguration().getInt("searchArgNum", 0)]; 
			searchValue = new String[searchIndex.length];
			String[] searchName = new String[searchIndex.length];
			
			//set the searchValue[] and searchIndex[] for further process in map
			for(int i = 0; i < searchIndex.length; i++) {
				searchName[i] = context.getConfiguration().get(Integer.toString(i)).split(":")[0];
				searchValue[i] = context.getConfiguration().get(Integer.toString(i)).split(":")[1];
			}
			for(int i = 0; i < searchName.length; i++) {
				int j = 0;
				for(; j < columnTitles.length; j++) {
					if(searchName[i].equals(columnTitles[j]))
						break;
				}
				searchIndex[i] = j;
			}
			
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String valueToString = value.toString().concat(",END");
			String[] splitValue = valueToString.split(",");
			
			int i = 0;
			for(; i < searchIndex.length; i++) {
				if(!searchValue[i].equals(splitValue[searchIndex[i]]))
					break;
			}
			if(i == searchIndex.length) {
				context.write(value, NullWritable.get());
			}
		}
	}
	
	public static class DrillDownReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			for(NullWritable val : values) {
				context.write(key, val);
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
		int rc = ToolRunner.run(conf, new DrillDown(), args);
		
		System.exit(rc);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String hdfsPath = args[1];
		String destPath = args[2];
		String fileName = args[3];
		
		for(int i = 4; i < args.length; i++) {
			getConf().set(Integer.toString(i - 4), args[i]);
		}
		getConf().setInt("searchArgNum", args.length - 4);
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(DrillDown.class);
		job.setMapperClass(DrillDownMapper.class);
		job.setReducerClass(DrillDownReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		FileSystem fs = FileSystem.get(getConf());

		fs.copyToLocalFile(false, new Path(hdfsPath + "part-r-00000"), new Path(destPath + fileName), true);
		fs.delete(new Path(hdfsPath), true);
		fs.close();
		
		return 0;
	}

}
