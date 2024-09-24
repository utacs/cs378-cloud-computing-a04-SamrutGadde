package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.xml.bind.v2.runtime.reflect.opt.Const;

public class Task1Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		IntWritable pickupErrorCounter = new IntWritable(0);
		IntWritable dropoffErrorCounter = new IntWritable(0);

		String[] line = value.toString().split(",");
		int pickupHour;
		int dropoffHour;

		try {
			String[] dateTime = line[2].split(" ");
			String[] hoursMinsSecs = dateTime[1].split(":");

			pickupHour = Integer.parseInt(hoursMinsSecs[0]) + 1;

			String[] dropoffDateTime = line[3].split(" ");
			String[] dropoffHoursMinsSecs = dropoffDateTime[1].split(":");

			dropoffHour = Integer.parseInt(dropoffHoursMinsSecs[0]) + 1;
		} catch (Exception e) {
			return;
		}

		try {
			double pickupGPSLong = Double.parseDouble(line[6]);
			double pickupGPSLat = Double.parseDouble(line[7]);

			if (pickupGPSLat == 0 || pickupGPSLong == 0) {
				System.out.println("Pickup: " + pickupGPSLong + " " + pickupGPSLat);
				pickupErrorCounter.set(1);
			}

		} catch (NumberFormatException e) {
			System.out.println(e.getMessage());
			pickupErrorCounter.set(1);
		}

		try {
			double dropoffGPSLong = Double.parseDouble(line[8]);
			double dropoffGPSLat = Double.parseDouble(line[9]);

			if (dropoffGPSLat == 0 || dropoffGPSLong == 0) {
				System.out.println("Dropoff: " + dropoffGPSLong + " " + dropoffGPSLat);
				dropoffErrorCounter.set(1);
			}

		} catch (NumberFormatException e) {
			System.out.println(e.getMessage());
			dropoffErrorCounter.set(1);
		}

		// System.out.println("Pickup Errors: " + pickupErrorCounter.get() + " Dropoff
		// Errors: " + dropoffErrorCounter.get());
		context.write(new IntWritable(pickupHour), pickupErrorCounter);
		context.write(new IntWritable(dropoffHour), dropoffErrorCounter);
	}

	// private boolean processLine(String[] splitLine) {
	// int commas = splitLine.length - 1;
	//
	// // if valid number of commas
	// try {
	// if (commas != 16) {
	// throw new IllegalArgumentException("Error: Invalid number of commas");
	// }
	//
	// // check 11th index and see if it can be converted into a double
	// DecimalFormat df = new DecimalFormat("#.##");
	//
	// BigDecimal bd = new BigDecimal(splitLine[16]).setScale(2,
	// RoundingMode.HALF_UP);
	// double localTotalAmount = bd.floatValue();
	// long localTotalSeconds = Long.parseLong(splitLine[4]);
	// double check = 0;
	// for (int sum = 11; sum <= 15; sum++) {
	// check += Float.parseFloat(splitLine[sum]);
	// }
	//
	// check = Float.parseFloat(df.format(check));
	//
	// if (check != localTotalAmount) {
	// throw new IllegalArgumentException("Error: Invalid total amount " +
	// localTotalAmount + " != " + check);
	// }
	//
	// if (check > 500) {
	// throw new IllegalArgumentException("Error: Total amount is greater than 500 "
	// + localTotalAmount);
	// }
	//
	// if (localTotalSeconds <= 0) {
	// throw new IllegalArgumentException("Error: Total Minutes/Seconds is 0 ");
	// }
	//
	// } catch (IllegalArgumentException e) {
	// // System.out.println(e.getMessage());
	// return false;
	// }
	//
	// return true;
	// }
}
