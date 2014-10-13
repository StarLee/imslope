package cn.edu.rg.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import cn.edu.rg.Item;
import cn.edu.rg.KeyPair;
import cn.edu.rg.KeyPairValue;
import cn.edu.rg.User;

public class Test {
	public static void setup() throws IOException {
		Path path = new Path("/slopetest/test.data");
		Path predict = new Path("/slopetest/predict.data");
		FileSystem fs = FileSystem.get(new Configuration());
		if (!fs.exists(path))
			fs.copyFromLocalFile(new Path("file:///opt/test.data"), path);// 从数据库直接得到的评分数据,备份在data/predict.data与data/test.data中
		if (!fs.exists(predict))
			fs.copyFromLocalFile(new Path("file:///opt/predict.data"), predict);// 通过分析用户之间的关系，物品之间的关系得到的对于一个用户对其要计算推荐的数据
	}

	/**
	 * 统计用户,以及item-item对
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Job statisticsUser() throws InterruptedException,
			ClassNotFoundException, IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("collect_records");
		job.setJarByClass(Test.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(UsersMapper.class);
		job.setMapOutputKeyClass(User.class);
		job.setMapOutputValueClass(Item.class);
		/*
		 * job.setReducerClass(UserItemsReducer.class);
		 * job.setOutputKeyClass(KeyPair.class);
		 * job.setOutputValueClass(KeyPairValue.class);
		 */
		job.setReducerClass(UserItemsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/slopetest/test.data"));
		Path path = new Path("/slopetest/output/sequence");
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(path))
			fs.delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		return job;
	}

	public static Job statistciItem(Path src_path) throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Test.class);
		job.setJobName("collect_diff_key");
		job.setNumReduceTasks(4);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(ItemDiffTextMapper.class);
		// job.setCombinerClass(ItemDiffTextCombiner.class);
		job.setReducerClass(ItemDiffTextReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		/*
		 * job.setMapOutputKeyClass(KeyPair.class);
		 * job.setMapOutputValueClass(KeyPairValue.class);
		 * job.setOutputFormatClass(SequenceFileOutputFormat.class);
		 * job.setMapperClass(ItemDiffMapper.class);
		 * job.setReducerClass(ItemDiffReducer.class);
		 * job.setOutputKeyClass(LongWritable.class);
		 * job.setOutputValueClass(Text.class);
		 */FileInputFormat.addInputPath(job, src_path);
		Path path = new Path("/slopetest/output/diff");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(path))
			fs.delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		return job;

	}

	public void statics() throws ClassNotFoundException, InterruptedException,
			IOException {
		Job staticsUser = Test.statisticsUser();
		Job staticsItem = Test.statistciItem(FileOutputFormat
				.getOutputPath(staticsUser));
		ControlledJob job_staticsUser = new ControlledJob(
				staticsUser.getConfiguration());
		ControlledJob job_staticsItem = new ControlledJob(
				staticsItem.getConfiguration());
		job_staticsItem.addDependingJob(job_staticsUser);
		JobControl jobControl = new JobControl("basic_statics");
		List<ControlledJob> allJobs = new ArrayList<ControlledJob>();
		allJobs.add(job_staticsUser);
		allJobs.add(job_staticsItem);
		jobControl.addJobCollection(allJobs);
		new Thread(jobControl).start();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// setup();
		// Job job=statisticsUser();
		// job.submit();
		// FileOutputFormat.getOutputPath(job);
		Path src_path = new Path("/slopetest/output/sequence/part-r-00000");
		Job job = statistciItem(src_path);
		job.submit();
		if (job.waitForCompletion(true)) {
			System.out.println(FileOutputFormat.getOutputPath(job).getName());
		}
	}
}
