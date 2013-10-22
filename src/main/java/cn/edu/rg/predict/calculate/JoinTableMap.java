package cn.edu.rg.predict.calculate;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.rg.KeyPair;

/**
 * 将评算表与差值表进行JOIN，得到真正计算的表
 * 
 * @author starlee
 * 
 */
public class JoinTableMap extends Mapper<LongWritable, Text, KeyPair, Text>
{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		String[] item = value.toString().split(":");
		int tag = Integer.parseInt(item[0]);
		// if(tag==0)//这是差值表的记录
		if (tag == 0)// 这是差值表的记录
		{
			KeyPair pair = new KeyPair();
			pair.setBaseKey(key);
			String compair = item[1];
			pair.setCompareKey(new LongWritable(Long.valueOf(compair)));
			context.write(pair, value);
		} else
		// 这个是评算表
		{
			long basic = Long.parseLong(item[1]);
			long calculate = Long.parseLong(item[3]);
			KeyPair pair = new KeyPair();
			if (basic < calculate)//以下是保证与差值表中的一致，因为差值对中只会有1－2，而不会出现2－1
			{
				pair.setBaseKey(new LongWritable(basic));
				pair.setCompareKey(new LongWritable(calculate));
			}
			else
			{
				pair.setBaseKey(new LongWritable(calculate));
				pair.setCompareKey(new LongWritable(basic));
			}
			context.write(pair, value);
		}

	}

}
