package geospatial1.operation1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.log4j.Logger;


public class HotspotDetection {
	final static Logger logger = Logger.getLogger(HotspotDetection.class);
	public static void main(String[] args) {
		//String[] arr = { "/home/dds/workspace/operation1/target/operation1-0.0.1-SNAPSHOT.jar" };
		SparkConf conf = new SparkConf().setAppName("spark-app");//.setMaster("spark://192.168.0.7:7077").setJars(arr);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, Long> cabinfo = sc.textFile(args[0], 1000)
				.filter(line -> line.charAt (0) != 'V')
				.map(s -> {
					String[] details = s.split(",");
					return new Info(details[1].trim(), details[6].trim(), details[5].trim());
				})
				.filter(info -> info.isValid())
				.mapToPair(info -> new Tuple2<String, Long>(info.getCoordinateKey(), 1L))
				.reduceByKey((a, b) -> a + b)
				.cache();
		logger.info("AMANDEBUG : sc.textFile is done");
		final double count = Double.valueOf(cabinfo.count());
		
		logger.info("AMANDEBUG : Count is done " + count);
		final double mean = Double.valueOf(cabinfo.map(t -> t._2()).reduce((a, b) -> a + b)) / Constants.TOTAL_CELLS;

		logger.info("AMANDEBUG : MEAN is done");
		final double std = Math
				.sqrt((cabinfo.map(t -> Math.pow(t._2(), 2)).reduce((a, b) -> a + b) / Constants.TOTAL_CELLS)
						- Math.pow(mean, 2));

		logger.info("AMANDEBUG : STD DEV is done " + Double.toString (mean) + " :::: " + std);

		final Map<String, Long> cabs = cabinfo.collectAsMap();

		List<Tuple2<String, Double>> top_50 = cabinfo.mapToPair(tuple -> {
			double weighted_sum = 0;
			double weights_sum = 0;
			List<String> coords = getListCoords(tuple._1());
			for (String s : coords) {
				weighted_sum += cabs.containsKey(s) ? cabs.get(s) : 0;
				weights_sum += 1;
			}
			double num = weighted_sum - (mean * weights_sum);
			double den = std * Math.sqrt(
					(Constants.TOTAL_CELLS * weights_sum - Math.pow(weights_sum, 2)) / (Constants.TOTAL_CELLS - 1));
			return new Tuple2<String, Double>(tuple._1(), num / den);
		}).map(t -> t.swap()).sortBy(t -> t._1(), false, 1).map(t -> t.swap()).take(50);
		List <String> ans = convertCoordinates (top_50);
		for (String i : ans ) {
			System.err.println(i);
		}
		sc.parallelize (ans).coalesce (1).saveAsTextFile (args[1]);
		sc.close();
	}

	private static List<String> getListCoords(String coord) {
		String[] split = coord.split(" ");
		int x = Integer.valueOf(split[0].trim());
		int y = Integer.valueOf(split[1].trim());
		int z = Integer.valueOf(split[2].trim());
		List<String> list = new ArrayList<>();

		for (int i = x - 1; i <= x + 1; i++) {
			for (int j = y - 1; j <= y + 1; j++) {
				for (int k = z - 1; k <= z + 1; k++) {
					if ((i >= Constants.MIN_LAT_COORD) && (i < Constants.MAX_LAT_COORD)
							&& (j >= Constants.MIN_LONG_COORD) && (j < Constants.MAX_LONG_COORD)
							&& (k >= Constants.MIN_DAY_COORD) && (k < Constants.MAX_DAY_COORD)) {
						list.add(Integer.toString(i) + " " + Integer.toString(j) + " " + Integer.toString(k));
					}
				}
			}

		}
		return list;
	}
	private static List <String> convertCoordinates ( List<Tuple2<String, Double>> t50) {
		List <String> ret = new ArrayList <String> ();
		for (Tuple2<String, Double> i : t50) {
			String[] split = i._1().split(" ");
			double x = Double.valueOf(split[0].trim())*0.01 + Constants.MIN_LAT;
			double y = Double.valueOf(split[1].trim())*0.01 + Constants.MIN_LONG;
			int z = Integer.valueOf(split[2].trim());
			String s = "" + x + "," + y + "," + z + "," + i._2();
			ret.add (s);
		}
		return ret;
	}

}
