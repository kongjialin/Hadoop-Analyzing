package train.examples.app;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class MapRedDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
		int rc = ToolRunner.run(conf, new MapRedDriver(), args);
		
		ExtraWork.generateAnotherJson(conf);
		ExtraWork.clearHdfs(conf);
		System.exit(rc);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		getConf().set("hdfsPath", args[1]);
		getConf().set("basePath", args[2]);
		getConf().set("jobID", args[3]);
		
		ExtraWork.setHdfsPath(args[1]);
		ExtraWork.setBasePath(args[2]);
		ExtraWork.setJobID(args[3]);
		
		ExtraWork.parseJson();
		
		getConf().addResource(new Path("/home/kong/usr/hadoop/hadoop-2.2.0/etc/hadoop/core-site.xml"));
		
		File csvDir = new File("/home/kong/temp/" + args[3] + "/csv/");
		File jsonDir = new File("/home/kong/temp/" + args[3] + "/json/");
		csvDir.mkdirs();
		jsonDir.mkdirs();
		csvDir.setWritable(true, false);
		jsonDir.setWritable(true, false);
		
		Job job = Job.getInstance(getConf());
		job.setJobName(ExtraWork.getJobName());
		job.setJarByClass(MapRedDriver.class);
		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		String[] fileNames = ExtraWork.generateFileNames();
		for(int i = 0; i < fileNames.length; i++) {
			MultipleOutputs.addNamedOutput(job, fileNames[i], TextOutputFormat.class, Text.class, Text.class);
		}
				
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

