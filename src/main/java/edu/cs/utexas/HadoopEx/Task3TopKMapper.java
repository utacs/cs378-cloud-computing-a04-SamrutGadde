package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

public class Task3TopKMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	private Logger logger = Logger.getLogger(Task3TopKMapper.class);

	private PriorityQueue<TaxiAndAverageEarning> pq;

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

		pq.add(new TaxiAndAverageEarning(new Text(key), new DoubleWritable(errorRate)));

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			TaxiAndAverageEarning wordAndCount = pq.poll();
			context.write(wordAndCount.getTaxiId(), wordAndCount.getAverageEarning());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}
