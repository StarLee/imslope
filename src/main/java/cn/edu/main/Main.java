package cn.edu.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.rg.calculate.TestCalculate;
import cn.edu.rg.mapred.Test;
import cn.edu.rg.predict.TestPredictPrepare;
import cn.edu.rg.predict.calculate.TestJoinTable;


public class Main {
	public void cascade() throws ClassNotFoundException, InterruptedException,
			IOException {
		Test.setup();// 把要处理的文件上传
		Job staticsUser = Test.statisticsUser();// 统计用户
		Job staticsItem = Test.statistciItem(FileOutputFormat
				.getOutputPath(staticsUser));// 统计项目之间的差值关系，目前是项目与项目之间的差值为中心
		/* 进行要计算的预测提前处理 */
		Job predictPrepare = TestPredictPrepare.execute();// 根据已经评分数据与将要进行预测的评分数据进行预处理计算，统一格式
		/* 转变为以用户为计算中心，类似于建立索引 */
		Job joinTable = TestJoinTable.beJoinTable(
				FileOutputFormat.getOutputPath(staticsItem),
				FileOutputFormat.getOutputPath(predictPrepare));// 合并差值表与预评分表，需要依赖差值与预测的结果
		/* 进行计算 */
		Job calculate = TestCalculate.execute(FileOutputFormat
				.getOutputPath(joinTable));
		ControlledJob job_staticsUser = new ControlledJob(
				staticsUser.getConfiguration());
		ControlledJob job_staticsItem = new ControlledJob(
				staticsItem.getConfiguration());
		job_staticsItem.addDependingJob(job_staticsUser);
		ControlledJob job_predictPrepare = new ControlledJob(
				predictPrepare.getConfiguration());
		ControlledJob job_joinTable = new ControlledJob(
				joinTable.getConfiguration());
		job_joinTable.addDependingJob(job_staticsItem);
		job_joinTable.addDependingJob(job_predictPrepare);
		ControlledJob job_calculate = new ControlledJob(
				calculate.getConfiguration());
		job_calculate.addDependingJob(job_joinTable);
		JobControl jobControl = new JobControl("basic_statics");
		List<ControlledJob> allJobs = new ArrayList<ControlledJob>();
		allJobs.add(job_staticsUser);
		allJobs.add(job_staticsItem);
		allJobs.add(job_predictPrepare);
		allJobs.add(job_joinTable);
		allJobs.add(job_calculate);
		jobControl.addJobCollection(allJobs);
		new Thread(jobControl).start();
		while (true) {
			if (jobControl.allFinished()) {
				System.out.println("process is over");
				break;
			}
		}
	}

	public static void main(String[] args) {
		Main cascade = new Main();
		try {
			cascade.cascade();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
