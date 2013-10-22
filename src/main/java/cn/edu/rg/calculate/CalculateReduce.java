package cn.edu.rg.calculate;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.rg.KeyPair;
import cn.edu.rg.predict.calculate.JoinTable;

public class CalculateReduce extends Reducer<KeyPair, JoinTable, KeyPair, FloatWritable>
{

	@Override
	protected void reduce(KeyPair key, Iterable<JoinTable> value,Context context)
			throws IOException, InterruptedException
	{
		int total=0;
		float rating=0;
		for(JoinTable table:value)
		{
			rating+=(table.getBasicRating()+table.getDiff())*(float)table.getTotalUser();
			total+=table.getTotalUser();
		}
		context.write(key, new FloatWritable(rating/(float)total));
	}

}
