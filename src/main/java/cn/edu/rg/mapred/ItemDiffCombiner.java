package cn.edu.rg.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cn.edu.rg.KeyPair;
import cn.edu.rg.KeyPairValue;

public class ItemDiffCombiner extends
		Reducer<KeyPair, KeyPairValue, KeyPair, KeyPairValue> {
	@Override
	protected void reduce(KeyPair key, Iterable<KeyPairValue> value,
			Context context) throws IOException, InterruptedException {
		float totalRating = 0;
		int totalUser = 0;
		for (KeyPairValue v : value)
		{
			totalRating += v.getDiff();
			totalUser+=v.getNumber();
		}
		KeyPairValue newKeyPairValue=new KeyPairValue();
		newKeyPairValue.setDiff(totalRating);
		newKeyPairValue.setNumber(totalUser);
	}
}
