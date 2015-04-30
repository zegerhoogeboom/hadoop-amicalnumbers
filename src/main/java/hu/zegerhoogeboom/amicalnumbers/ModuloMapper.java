package hu.zegerhoogeboom.amicalnumbers;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Zeger Hoogeboom
 */
public class ModuloMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		int original = Integer.parseInt(value.toString());
		int number = Integer.parseInt(value.toString()) - 1;
		while (number > 0) {
			int dividable = original % number;
			if (dividable == 0) {
				context.write(new LongWritable(original), new LongWritable(number));
			}
			number--;
		}
	}
}
