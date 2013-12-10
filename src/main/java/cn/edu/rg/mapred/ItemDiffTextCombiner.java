package cn.edu.rg.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.rg.KeyPairValue;

public class ItemDiffTextCombiner extends
		Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context) throws IOException, InterruptedException {
		float totalRating = 0;
		int totalUser = 0;
		for (Text v : value)
		{
			totalRating += Float.parseFloat(v.toString().split(":")[0]);
			totalUser++;
		}
		Text valueText=new Text();
		StringBuffer valuebuffer=new StringBuffer();
		valuebuffer.append(totalRating);
		valuebuffer.append(":");
		valuebuffer.append(totalUser);
		valueText.set(valuebuffer.toString());
		context.write(key, valueText);
	}
}
