package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

public class Task2TopKMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	private Logger logger = Logger.getLogger(Task2TopKMapper.class);

	private PriorityQueue<TaxiIdAndErrorRate> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		double errorRate = Double.parseDouble(value.toString());

		pq.add(new TaxiIdAndErrorRate(new Text(key), new DoubleWritable(errorRate)));

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			TaxiIdAndErrorRate wordAndCount = pq.poll();
			context.write(wordAndCount.getTaxiId(), wordAndCount.getErrorRate());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}
