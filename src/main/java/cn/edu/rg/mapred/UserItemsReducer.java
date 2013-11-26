package cn.edu.rg.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import cn.edu.rg.Item;
import cn.edu.rg.KeyPair;
import cn.edu.rg.KeyPairValue;
import cn.edu.rg.User;

public class UserItemsReducer extends Reducer<User, Item, KeyPair, KeyPairValue> {

	private Logger log=LogManager.getLogger(UserItemsReducer.class);
	@Override
	protected void reduce(User key, Iterable<Item> value, Context context)
			throws IOException, InterruptedException {
		
		//value这个迭代器只能迭代一次就会被清空,即使通过context.getValues()也是一次使用后消失
		//所以这个地方先把结果取出来放到内存然后才可以重复利用,有一个问题是如果内存不够用怎么办，所以这个地方应该是两个同样（在map上write两次）的文件，标识来自己不同的文件
		//以后这个地方得优化，不能每一次都计算，应该去掉一些，另外一个用户的所有申请的项目记录都不是太多，也就是说这个矩阵其实很稀疏
		List<Item> list=new ArrayList<Item>();
		
		int n=0;
		Iterator<Item> i=value.iterator();
		while(i.hasNext())
		{
			Item ie=i.next();
			Item item=new Item();
			item.setId(ie.getId());
			item.setRating(ie.getRating());
			list.add(item);
		}
		log.info(key.getId()+":"+list.size()+":start");
		Iterator<Item> it=list.iterator();
		while(it.hasNext())
		{
			Item basic=it.next();
			//log.info(key.getId()+":"+basic.getId()+":"+basic.getRating());
			//context.write(new Text(Long.toString(key.getId())), new FloatWritable(basic.getRating()));
			Iterator<Item> itt=list.iterator();
			while(itt.hasNext())
			{
				Item compare=itt.next();
				//log.info("---"+basic.getId()+":"+compare.getId());
				if(basic.getId()>=compare.getId())//始终只保证小的减大的，本身差不保存，大的减小的不保存,即保证是上三角或下三角
					continue;
				float diff=compare.getRating()-basic.getRating();
				KeyPair keypair=new KeyPair();
				keypair.setBaseKey(new LongWritable(basic.getId()));
				keypair.setCompareKey(new LongWritable(compare.getId()));
				KeyPairValue keyPairValue=new KeyPairValue();
				//keyPairValue.setBaseRating(basic.getRating());
				//keyPairValue.setCompareRating(compare.getRating());
				keyPairValue.setDiff(diff);
				//keyPairValue.setUser(key.getId());
				n++;
				context.write(keypair, keyPairValue);
			}
			
		}
		log.info(key.getId()+"----"+n+":end");
	}


}
