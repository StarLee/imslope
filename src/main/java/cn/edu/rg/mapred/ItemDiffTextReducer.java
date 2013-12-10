package cn.edu.rg.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import cn.edu.rg.ItemDiffInfo;
import cn.edu.rg.KeyPair;
import cn.edu.rg.KeyPairValue;
/**
 * 形成差值表
 * @author starlee
 *
 */
public class ItemDiffTextReducer extends
		Reducer<Text, Text, LongWritable, Text>
{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context) throws IOException, InterruptedException
	{
		float totalRating = 0;
		long totalUser = 0;
		//List<KeyPairValue> ls = new ArrayList<KeyPairValue>();//像这种内存性的东西，真的怀疑如果这个key中有很多很多value，那要如何处理，比如这个KEY对应10w
		for (Text v : value)
		{
			/*KeyPairValue newValue=new KeyPairValue();
			newValue.setBaseRating(v.getBaseRating());
			newValue.setCompareRating(v.getCompareRating());
			newValue.setDiff(v.getDiff());
			newValue.setUser(v.getUser());*/
			String[] ratingValue=v.toString().split(":");//前面部分是评分，后面是用户数
			totalRating += Float.parseFloat(ratingValue[0]);
			totalUser+=Integer.parseInt(ratingValue[1]);
			//ls.add(newValue);
		}
		/*ItemDiffInfo info = new ItemDiffInfo();
		info.setAverageRating(totalRating / (float) totalUser);
		info.setTotalRating(totalRating);
		info.setTotalUser(totalUser);
		context.write(key, info);*/
		//由于要保持数据格式一致故上面的形式不能正常
		String[] keys=key.toString().split(":");
		LongWritable basicKey=new LongWritable(Long.parseLong(keys[0]));
		StringBuilder str=new StringBuilder("0:");//这是tag标记
		str.append(keys[1]);
		str.append(":");
		str.append(totalRating);
		str.append(":");
		str.append(totalUser);
		str.append(":");
		str.append(totalRating / (float) totalUser);
		context.write(basicKey, new Text(str.toString()));
		/*for (KeyPairValue pairValue : ls)
		{
			ItemDiffInfo info = new ItemDiffInfo();
			info.setAverageRating(totalRating / (float) totalUser);
			info.setTotalRating(totalRating);
			info.setTotalUser(totalUser);
			info.setBasicRating(pairValue.getBaseRating());
			info.setCompairRating(pairValue.getCompareRating());
			info.setUser(pairValue.getUser());
			context.write(key, info);
		}*/
	}

}
