package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		String taxiId = line[0];
		int counter = 0;

		try {
			double pickupGPSLong = Double.parseDouble(line[6]);
			double pickupGPSLat = Double.parseDouble(line[7]);
			double dropoffGPSLong = Double.parseDouble(line[8]);
			double dropoffGPSLat = Double.parseDouble(line[9]);

			if (pickupGPSLat == 0 || pickupGPSLong == 0 || dropoffGPSLat == 0 || dropoffGPSLong == 0) {
				counter = 1;
			}

		} catch (NumberFormatException e) {
			System.out.println(e.getMessage());
			counter = 1;
		}

		Text errorAndTotal = new Text(counter + "," + 1);

		context.write(new Text(taxiId), errorAndTotal);

	}
}
