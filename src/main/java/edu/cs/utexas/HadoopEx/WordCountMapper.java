package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Integer, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		// extract hour for pickup time
		String[] dateTime = line[2].split(" ");
		String[] hoursMinsSecs = dateTime[1].split(":");
		int pickupHour = Integer.parseInt(hoursMinsSecs[0]);

		
		// extract hour for dropoff time
		String[] dateTimeDrop = line[3].split(" ");
		String[] hoursMinsSecsDrop = dateTimeDrop[1].split(":");
		int dropHour = Integer.parseInt(hoursMinsSecsDrop[0]);

		// calculate errors for pickup
		int pickupError = 0;

		try
		{
			double pickupGPSLong = Double.parseDouble(line[6]);
			double pickupGPSLat = Double.parseDouble(line[7]);
			if(pickupGPSLong == 0)
			{
				pickupError++;
			}

			if(pickupGPSLat == 0)
			{
				pickupError++;
			}

		} catch (Exception e) {
			// potentially an empty string
			pickupError++;
		}

		// calculate errors for pickup
		int dropOffError = 0;

		try
		{
			double dropoffGPSLong = Double.parseDouble(line[8]);
			double dropoffGPSLat = Double.parseDouble(line[9]);
			if(dropoffGPSLong == 0)
			{
				dropOffError++;
			}

			if(dropoffGPSLat == 0)
			{
				dropOffError++;
			}

		} catch (Exception e) {
			// potentially an empty string
			dropOffError++;
		}

		// write both
		context.write(pickupHour, new IntWritable(pickupError));
		context.write(dropHour, new IntWritable(dropOffError));

	}
}