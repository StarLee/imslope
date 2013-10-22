package cn.edu.rg.predict;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictMapper extends Mapper<LongWritable, Text, LongWritable, ItemPredictStatus>
{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException
	{
		String[] records=value.toString().split("\t");
		ItemPredictStatus status=new ItemPredictStatus();
		if(records.length>=4)//评分表
		{
			String userID=records[0];//用户ID
			String itemID=records[1];//项目ID
			String rating=records[2];//评分
			long user=Long.parseLong(userID);
			status.setTag((byte)0x0);
			status.setItem(Long.parseLong(itemID));
			status.setUser(Long.parseLong(userID));
			status.setRating(Float.parseFloat(rating));
			LongWritable ky=new LongWritable();
			ky.set(user);
			context.write(ky,status);
		}
		else//预评分表
		{
			String userID=records[0];//用户ID
			String itemID=records[1];//项目ID
			long user=Long.parseLong(userID);
			status.setTag((byte)0x1);
			status.setItem(Long.parseLong(itemID));
			status.setUser(Long.parseLong(userID));
			status.setRating(0);
			LongWritable ky=new LongWritable();
			ky.set(user);
			context.write(ky, status);
		}
	}

}
