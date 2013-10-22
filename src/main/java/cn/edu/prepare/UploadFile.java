package cn.edu.prepare;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;


import cn.edu.rg.Item;
import cn.edu.rg.User;
/**
 * 上传已经评分的人
 * @author starlee
 *
 */
public class UploadFile
{
	public static void uploadHasRating() throws IOException
	{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		Path rating=new Path("/slopetest/rating/test");
		SequenceFile.Writer write=new SequenceFile.Writer(fs, conf,rating, User.class, Item.class);
		java.io.File file=new java.io.File("/home/starlee/test.data");
		FileInputStream inputstream=new FileInputStream(file);
		DataInputStream in = new DataInputStream(inputstream);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		while(true)
		{
			String line=reader.readLine();
			if(line==null)
				break;
			else
			{
				String[] records=line.split("\t");
				String userID=records[0];//用户ID
				String itemID=records[1];//项目ID
				String rate=records[2];//用户对其评分
				User user=new User();
				user.setId(Long.parseLong(userID));
				Item itemrecord=new Item();
				itemrecord.setId(Long.parseLong(itemID));
				itemrecord.setRating(Float.parseFloat(rate));
				write.append(user, itemrecord);
			}
		}
	}
	public static void main(String[] args) throws IOException
	{
		UploadFile.uploadHasRating();
	}
}
