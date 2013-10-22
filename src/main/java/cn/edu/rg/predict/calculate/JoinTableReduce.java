package cn.edu.rg.predict.calculate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.rg.KeyPair;
/**
 * 除去根本就无法做到的评分项，比如I1－I4，假设他们之间的差值不存在，那任何用户都不能利用I1的值来预测I4，只能能过I2，I3等才行,后面计算时都是针对这个开始
 * @author starlee
 *
 */
public class JoinTableReduce extends Reducer<KeyPair, Text, LongWritable, JoinTable>
{

	@Override
	protected void reduce(KeyPair key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException
	{
		List<String> calculateList=new ArrayList<String>();//如果内存放不下去
		List<String> diffList=new ArrayList<String>();//正常只有一个，不会有多个,0个则不需要计算
		for(Text value:values)
		{
			String item=value.toString();
			String[] fields=item.split(":");
			int tag = Integer.parseInt(fields[0]);
			if(tag==0)//差值表，对一个Key中只有一个
			{
				diffList.add(item);
			}
			else//评算表
			{
				calculateList.add(item);
			}
		}
		if(diffList.size()==0)//没有差值信息，也就代表，这条路是不可以计算的
		{
			
		}
		else//是可以计算的
		{
			if(calculateList.size()==0)//没有人想计算这两者的关系
			{
				
			}
			else
			{
				Iterator<String> cal=calculateList.iterator();
				while(cal.hasNext())
				{
					String record=cal.next();
					Iterator<String> diff=diffList.iterator();
					while(diff.hasNext())
					{
						String diffRecord=diff.next();
						String[] diffFields=diffRecord.split(":");
						String[] calFields=record.split(":");
						long targetItem=Long.valueOf(calFields[3]);//要计算的项目
						long basicItem=Long.valueOf(calFields[1]);//基础项目
						float basicRating=Float.valueOf(calFields[2]);//用户对基础项的评分
						long user=Long.valueOf(calFields[4]);//评分的用户
						long totaluser=Long.valueOf(diffFields[3]);//对这项差值的总共用户
						float diffRating=Float.valueOf(diffFields[4]);//对这个key的差值
						if(basicItem>targetItem)
							diffRating=(float)((float)0-diffRating);
						JoinTable join=new JoinTable(basicItem, targetItem, user, basicRating, diffRating, totaluser);
						context.write(new LongWritable(user), join);
					}
				}
			}
		}
	}
}
