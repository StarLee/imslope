package cn.edu.rg.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.rg.Item;
import cn.edu.rg.User;

public class UsersMapper extends Mapper<LongWritable, Text, User, Item> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			String line=value.toString();
			String[] records=line.split("\t");
			
			String userID=records[1];//用户ID
			String itemID=records[0];//项目ID
			String rating=records[2];//用户对其评分
			User user=new User();
			user.setId(Long.parseLong(userID));
			Item itemrecord=new Item();
			itemrecord.setId(Long.parseLong(itemID));
			itemrecord.setRating(Float.parseFloat(rating));
			context.write(user, itemrecord);//对于里面的每一条记录写用户与值的形式，输出给reduce针对每一个user处理

	}

}
