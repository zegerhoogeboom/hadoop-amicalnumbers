package hu.zegerhoogeboom.amicalnumbers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zeger Hoogeboom
 */
public class AmicalReducer  extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
{
	static Map<LongWritable, LongWritable> results = new HashMap<LongWritable, LongWritable>();

	private LongWritable result = new LongWritable();

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		if (results.containsKey(result) || results.containsValue(key)) {
			context.write(key, result);
		}
		results.put(key, result);
	}
}
