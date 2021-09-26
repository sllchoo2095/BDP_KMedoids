package edu.rmit.cosc2367.s3846487.Task3Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;



import edu.rmit.cosc2367.s3846487.distance.DistanceMeasure;
import edu.rmit.cosc2367.s3846487.distance.EuclidianDistance;
import edu.rmit.cosc2367.s3846487.model.Medoid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3846487.model.DataPoint;

/**
 * First generic specifies the type of input Key.
 * Second generic specifies the type of input Value.
 * Third generic specifies the type of output Key.
 * Last generic specifies the type of output Value.
 * In this case, the input key-value pair has the same type with the output one.
 *
 * The difference is that the association between a centroid and a data-point may change.
 * This is because the centroids has been recomputed in previous reduce().
 */
public class KMedoidsMapper extends Mapper<Object, Text, Medoid, DataPoint> {
	
	private static final Logger LOG = Logger.getLogger(KMedoidsMapper.class);

	private final List<Medoid> medoids = new ArrayList<>();
	private DistanceMeasure distanceMeasurer;

	/**
	 *
	 * In this method, all centroids are loaded into memory as, in map(), we are going to compute the distance
	 * (similarity) of the data point with all centroids and associate the data point with its nearest centroid.
	 * Note that we load the centroid file on our own, which is not the same file as the one that hadoop loads in map().
	 *
	 *
	 * @param context Think of it as a shared data bundle between the main class, mapper class and the reducer class.
	 *                One can put something into the bundle in KMeansClusteringJob.class and retrieve it from there.
	 *
	 */
    @SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		// We get the URI to the centroid file on hadoop file system (not local fs!).
		// The url is set beforehand in KMeansClusteringJob#main.
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		// After having the location of the file containing all centroids data,
		// we read them using SequenceFile.Reader, which is another API provided by hadoop for reading binary file
		// The data is modeled in Centroid.class and stored in global variable centers, which will be used in map()
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
			Medoid key = new Medoid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Medoid medoid = new Medoid(key);
				medoid.setClusterIndex(index++);
				medoids.add(medoid);
			}
		}

		// This is for calculating the distance between a point and another (centroid is essentially a point).
		distanceMeasurer = new EuclidianDistance();
	}

	/**
	 *
	 * After everything is ready, we calculate and re-group each data-point with its nearest centroid,
	 * and pass the pair to reducer.
	 *
	 * @param centroid key
	 * @param dataPoint value
	 */
	//@Override
	protected void map(Object medoid, Text textInput, Context context) throws IOException,
			InterruptedException {

		Medoid nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		
		ArrayList<DataPoint> dataPoints = new ArrayList<DataPoint>(); 
		
		LOG.setLevel(Level.DEBUG);
		
		//Make sure every line is split. 
		StringTokenizer inputs = new StringTokenizer(textInput.toString(), "\n"); 
		
		while (inputs.hasMoreTokens()) {
			String line= inputs.nextToken(); 
			
			StringTokenizer lineBreaker = new StringTokenizer(line); 
			
			ArrayList<String> pointsStr = new ArrayList<String>();
			
			while(lineBreaker.hasMoreTokens()) {
				
				pointsStr.add(lineBreaker.nextToken()); 
			}
			
			dataPoints.add(new DataPoint(Double.parseDouble(pointsStr.get(0)), Double.parseDouble(pointsStr.get(1)))); 
			
			LOG.debug("dataPoint = "+ dataPoints.get(0));
			LOG.debug("Size of dataPoints ArrayList = "+ dataPoints.size());
			LOG.debug("Medoids size = "+ medoids.size());
			
		for(DataPoint data: dataPoints) {
				
				for (Medoid m: medoids) {
					 double dist= distanceMeasurer.measureDistance(m.getCenterVector(), data.getVector());
					 
					 if (dist< nearestDistance|| nearest == null) {
						 nearestDistance = dist; 
						 nearest = m; 
					 }
				}
				
				LOG.debug("MAP Medoid = "+ nearest.toString()+ " MAP dataPoint = "+ data);
				context.write(nearest, data);
			}//for
			
	
				
			
		}//map
	}
		
		
	}//KmedoidsMapper
	




	
//	@Override
//	protected void map(Medoid medoid, DataPoint dataPoint, Context context) throws IOException,
//			InterruptedException {
//
//		Medoid nearest = null;
//		double nearestDistance = Double.MAX_VALUE;
////Loop through every data point
//		for (Medoid m : medoids) {
//			//todo: find the nearest centroid for the current dataPoint, pass the pair to reducer
//			
//			double dist = distanceMeasurer.measureDistance(m.getCenterVector(), dataPoint.getVector());
//			if (dist < nearestDistance || nearest == null) {
//				nearestDistance = dist;
//				nearest = m;
//				dataPoint.setClusterIndex(m.getClusterIndex());//set the clusterIndex of the data point. 
//			}
//		}
//
//		context.write(nearest, dataPoint);
//	}


