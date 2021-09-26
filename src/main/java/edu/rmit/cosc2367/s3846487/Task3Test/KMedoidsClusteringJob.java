package edu.rmit.cosc2367.s3846487.Task3Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3846487.model.DataPoint;
import edu.rmit.cosc2367.s3846487.model.Medoid;

/**
 * K-means algorithm in mapReduce<p>
 *
 * Terminology explained:
 * - DataPoint: A dataPoint is a point in 2 dimensional space. we can have as many as points we want, and we are going to group
 * 				those points that are similar( near) to each other.
 * - cluster: A cluster is a group of dataPoints that are near to each other.
 * - Centroid: A centroid is the center point( not exactly, but you can think this way at first) of the cluster.
 *
 * Files involved:
 * - data.seq: It contains all the data points. Each chunk consists of a key( a dummy centroid) and a value(data point).
 * - centroid.seq: It contains all the centroids with random initial values. Each chunk consists of a key( centroid) and a value( a dummy int)
 * - depth_*.seq: These are a set of directories( depth_1.seq, depth_2.seq, depth_3.seq ... ), each of the directory will contain the result of one job.
 * 				  Note that the algorithm works iteratively. It will keep creating and executing the job before all the centroid converges.
 * 				  each of these directory contains files which is produced by reducer of previous round, and it is going to be fed to the mapper of next round.
 * Note, these files are binary files, and they follow certain protocals so that they can be serialized and deserialized by SequenceFileOutputFormat and SequenceFileInputFormat
 *
 * This is an high level demonstration of how this works:
 *
 * - We generate some data points and centroids, and write them to data.seq and cen.seq respectively. We use SequenceFile.Writer so that the data
 * 	 could be deserialize easily.
 *
 * - We start our first job, and feed data.seq to it, the output of reducer should be in depth_1.seq. cen.seq file is also updated in reducer#cleanUp.
 * - From our second job, we keep generating new job and feed it with previous job's output( depth_1.seq/ in this case),
 * 	 until all centroids converge.
 *
 */
public class KMedoidsClusteringJob {

	private static final Logger LOG = Logger.getLogger(KMedoidsClusteringJob.class);
	
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {

		LOG.setLevel(Level.INFO);
		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
	
		Path inputDataPath = new Path(args[0]); //new arguement for the input path 
		Path centroidDataPath = new Path("clustering/centroid.seq");
		conf.set("centroid.path", centroidDataPath.toString());
		Path outputDir = new Path(args[1]+"/clustering/depth_1");//Explicitly state output directory of first iteration 
		int clusterNum = Integer.parseInt(args[2]); //number of cluster or k value 
		
	
		

		Job job = Job.getInstance(conf);
		job.setJobName("KMedoids Clustering ");

		job.setMapperClass(KMedoidsMapper.class);
		job.setReducerClass(KMedoidsReducer.class);
		job.setJarByClass(KMedoidsMapper.class);

		FileInputFormat.addInputPath(job, inputDataPath);
		FileSystem fs = FileSystem.get(conf);


		if (fs.exists(centroidDataPath)) {
			fs.delete(centroidDataPath, true);
		}

		generateMedoid(conf, centroidDataPath, fs, clusterNum);

		job.setNumReduceTasks(1);
		
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Medoid.class);
		job.setMapOutputValueClass(DataPoint.class);
		job.setOutputKeyClass(Medoid.class);
		job.setOutputValueClass(DataPoint.class);
		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(KMedoidsReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", centroidDataPath.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMedoids Clustering = " + iteration);
			
			job.setMapperClass(KMedoidsMapper.class);
			job.setMapOutputKeyClass(Medoid.class);
			job.setMapOutputValueClass(DataPoint.class);
			job.setReducerClass(KMedoidsReducer.class);
			job.setJarByClass(KMedoidsMapper.class);

			
			outputDir = new Path(args[1]+"/clustering/depth_" + iteration); //arg[1]
		

			FileInputFormat.addInputPath(job, inputDataPath);
			if (fs.exists(outputDir))
				fs.delete(outputDir, true);

			FileOutputFormat.setOutputPath(job, outputDir);

			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Medoid.class);
			job.setMapOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);
			job.waitForCompletion(true);
			iteration++;
			
			//step 4 start
			counter = job.getCounters().findCounter(KMedoidsReducer.Counter.CONVERGED).getValue();
			//step 4 end
		}

	}
	
	
	@SuppressWarnings("deprecation")
	public static void generateDataPoints(Configuration conf, Path in, FileSystem fs, Path inputDataPath) throws IOException {
		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Medoid.class,
				DataPoint.class)) {
		
			try(FSDataInputStream input = fs.open(inputDataPath)){
				BufferedReader br = new BufferedReader(new InputStreamReader(input)); 
				
				String str; 
				
				while((str = br.readLine())!=null) {
					String dataPoints[] = str.split("\\s+");
					
					dataWriter.append(new Medoid(new DataPoint(0,0)),new DataPoint(Double.parseDouble(dataPoints[0]), Double.parseDouble(dataPoints[1])));
					
				}
				
				br.close();
				input.close();

				
			}
		}
	}//generateDataPoints
	
	
	//Step 1 start
	@SuppressWarnings("deprecation")
	public static void generateMedoid(Configuration conf, Path center, FileSystem fs, int clusterNum) throws IOException {
		final Logger LOG = Logger.getLogger(KMedoidsClusteringJob.class);
		
		try (SequenceFile.Writer medoidWriter = SequenceFile.createWriter(fs, conf, center, Medoid.class,
				IntWritable.class)) {
			final IntWritable zero = new IntWritable(0);
			
			DataPoint kNum [] = {
					new DataPoint(-73.981636047363281,40.732769012451172),//line 100 -73.981636047363281 40.732769012451172
					new DataPoint(-73.987045288085938,40.760906219482422), //line 200 -73.987045288085938,40.760906219482422
					new DataPoint(-73.979301452636719,40.767261505126953),//line 300 -73.979301452636719 40.767261505126953
					new DataPoint(-73.964279174804688,40.768009185791016),//line 400 -73.964279174804688,40.768009185791016
					new DataPoint(-73.984382629394531,40.745979309082031),//Line 500 -73.984382629394531 40.745979309082031
					new DataPoint(-73.980377197265625,40.777301788330078)//line 600 -73.980377197265625 40.777301788330078
			}; 
			
			for (int i =0; i<clusterNum; i++) {
				
				LOG.info("Adding Medoid = "+ kNum[i]);
				
				medoidWriter.append(new Medoid(kNum[i]), zero);
				
				
			}
			
			
		}
	}//GenerateCentroid
	
	//Step 1 end 


}
