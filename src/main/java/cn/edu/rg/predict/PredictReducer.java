package cn.edu.rg.predict;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 形成评分预测表
 * @author starlee
 *
 */
public class PredictReducer extends
		Reducer<LongWritable, ItemPredictStatus, LongWritable, Text>
{

	private Set<LongWritable> has;
	private Set<LongWritable> noHas;
	private List<LongWritable> toCalculate;// 用来存储那些此Key的用户要真正评价的项目
	private List<ItemPredictStatus> hasRating;//用来储存那些此key的用户已经评价的项目
	private Logger log=LogManager.getLogger(PredictReducer.class);
	@Override
	protected void reduce(LongWritable key, Iterable<ItemPredictStatus> value,
			Context context) throws IOException, InterruptedException
	{
		this.has = new TreeSet<LongWritable>();
		this.noHas = new TreeSet<LongWritable>();
		this.hasRating=new ArrayList<ItemPredictStatus>();
		this.toCalculate = new ArrayList<LongWritable>();
		Iterator<ItemPredictStatus> it = value.iterator();
		while (it.hasNext())
		{
			ItemPredictStatus predict = it.next();
			if (predict.getTag() == (byte) 0x0)// 则这个特定用户（key）是评分表的
			{
				LongWritable lw = new LongWritable();
				lw.set(predict.getItem());
				has.add(lw);
				ItemPredictStatus item=new ItemPredictStatus();
				item.setItem(predict.getItem());
				item.setRating(predict.getRating());
				item.setUser(predict.getUser());
				item.setTag(predict.getTag());
				this.hasRating.add(item);
			} else
			// 特定用户预测表中的
			{
				LongWritable lw = new LongWritable();
				lw.set(predict.getItem());
				if (has.contains(lw))// 初次找一下
				{
					continue;
				} else
					noHas.add(lw);
			}
		}
		if (noHas.size() != 0)// 再一次选一下，输出不存的
		{
			Iterator<LongWritable> spare = noHas.iterator();
			while (spare.hasNext())
			{
				LongWritable compare = spare.next();
				if (!has.contains(compare))
				{
					this.toCalculate.add(compare);
				}
			}
		}
		
		//log.info("result-------------------"+key.get()+"--:--"+toCalculate.size());
		Iterator<LongWritable> toc = toCalculate.iterator();
		while (toc.hasNext())
		{
			LongWritable com=toc.next();
			//log.info("detail-------------------"+key.get()+"--:--"+com.get());
			Iterator<ItemPredictStatus> itt = hasRating.iterator();
			while (itt.hasNext())
			{
				/*ItemPredictStatus basic=itt.next();
				ItemCalculate to=new ItemCalculate();
				to.setBasic(basic.getItem());
				to.setBasicRating(basic.getRating());
				to.setCalculate(com.get());
				to.setUser(key.get());
				context.write(new LongWritable(key.get()), to);*/
				//因为格式不统一，上面的方式不好,注意以下str的格式
				ItemPredictStatus basic=itt.next();
				StringBuilder str=new StringBuilder("1:");//这是tag标记
				str.append(basic.getItem());//基础项
				str.append(":");//分隔符
				str.append(basic.getRating());//评分
				str.append(":");//分隔符
				str.append(com.get());//比较项
				str.append(":");
				str.append(key.get());//用户
				context.write(new LongWritable(key.get()), new Text(str.toString()));
			}
		}
	}
}
