package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;

public class Task3TopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private PriorityQueue<TaxiAndAverageEarning> pq = new PriorityQueue<TaxiAndAverageEarning>(10);

	private Logger logger = Logger.getLogger(Task3TopKReducer.class);

	// public void setup(Context context) {
	//
	// pq = new PriorityQueue<TaxiIdAndErrorRate>(10);
	// }

	/**
	 * Takes in the topK from each mapper and calculates the overall topK
	 * 
	 * @param text
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		// A local counter just to illustrate the number of values here!
		int counter = 0;

		// size of values is 1 because key only has one distinct value
		for (DoubleWritable value : values) {
			counter = counter + 1;
			logger.info("Reducer Text: counter is " + counter);
			logger.info("Reducer Text: Add this item " + new TaxiIdAndErrorRate(key,
					value).toString());

			pq.add(new TaxiAndAverageEarning(new Text(key), new DoubleWritable(value.get())));

			logger.info("Reducer Text: " + key.toString() + " , Count: " +
					value.toString());
			logger.info("PQ Status: " + pq.toString());
		}

		// keep the priorityQueue size <= heapSize
		while (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("TopKReducer cleanup cleanup.");
		logger.info("pq.size() is " + pq.size());

		List<TaxiAndAverageEarning> values = new ArrayList<TaxiAndAverageEarning>(10);

		while (pq.size() > 0) {
			values.add(pq.poll());
		}

		logger.info("values.size() is " + values.size());
		logger.info(values.toString());

		// reverse so they are ordered in descending order
		Collections.reverse(values);

		for (TaxiAndAverageEarning value : values) {
			context.write(value.getTaxiId(), value.getAverageEarning());
			logger.info("TopKReducer - Top-10 Words are: " + value.getTaxiId() + " Count:" +
					value.getAverageEarning());
		}
	}

}
