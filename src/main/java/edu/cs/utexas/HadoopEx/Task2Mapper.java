package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.xml.bind.v2.runtime.reflect.opt.Const;

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
