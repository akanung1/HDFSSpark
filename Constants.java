package geospatial1.operation1;

public class Constants {
	public static final double MAX_LAT = 40.9;
	public static final double MIN_LAT = 40.5;
	public static final double MAX_LONG = -73.7;
	public static final double MIN_LONG = -74.25;
	public static final int MIN_LAT_COORD = 0;
	public static final int MAX_LAT_COORD = (int) Math.round((MAX_LAT - MIN_LAT) * 100) + 1;
	public static final int MIN_LONG_COORD = 0;
	public static final int MAX_LONG_COORD = (int) Math.round((MAX_LONG - MIN_LONG) * 100) + 1;
	public static final int MIN_DAY_COORD = 0;
	public static final int MAX_DAY_COORD = 31;
	public static final int TOTAL_CELLS = 31 * 55 * 40;

}
