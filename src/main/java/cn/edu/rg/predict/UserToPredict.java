package cn.edu.rg.predict;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class UserToPredict extends
		Reducer<FloatWritable, FloatWritable, FloatWritable, FloatWritable>
{
	private Set<ItemPredictStatus> set;

	@Override
	protected void reduce(FloatWritable key, Iterable<FloatWritable> value,
			Context context) throws IOException, InterruptedException
	{
		Iterator<FloatWritable> it = value.iterator();
		byte status=0x0;
		while (it.hasNext())
		{
			
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException
	{
		/*Path[] paths = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());// 取出那些可以预测的项目（进行这一步主要是去掉那些已经打分的）,正常每一个用户的最大集一般是不一样的，可以通过内容分析得出，这样就可以大大的去掉好多数据，那样建立的表就可以直接mapreduce了，现在是假设对所有数据都得预测
		FileSystem fs = FileSystem.get(context.getConfiguration());
		this.set = new TreeSet<ItemPredictStatus>();
		for (Path path : paths)
		{
			DataInputStream in = fs.open(path);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			ItemPredictStatus itemPredictStatus=new ItemPredictStatus();
			byte status=0x0;
			itemPredictStatus.setTag(status);
			itemPredictStatus.setItem(Float.valueOf(reader.readLine()));
			this.set.add(itemPredictStatus);
		}*/
	}
}