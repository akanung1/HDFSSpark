package geospatial1.operation1;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.lang.StringBuilder;

public class Info implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4362327066692758115L;
	private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
	public final Date pickup_time;
	public final double latitude;
	public final double longitude;

	final private int x;
	final private int y;
	final private int z;

	public Info(String date, String latitude, String longitude) {
		try {
			format.setTimeZone(TimeZone.getTimeZone("America/New_York"));
			this.pickup_time = format.parse(date);
		} catch (ParseException e) {
			throw new RuntimeException();
		}
		this.latitude = Double.valueOf(latitude);
		this.longitude = Double.valueOf(longitude);
		this.x = (int) Math.floor((this.latitude - Constants.MIN_LAT) * 100);
		this.y = (int) Math.floor((this.longitude - Constants.MIN_LONG) * 100);
		Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
		cal.setTime(pickup_time);
		this.z = cal.get(Calendar.DAY_OF_MONTH) - 1;
	}

	public boolean isValid() {
		return ((this.x >= Constants.MIN_LAT_COORD) && (this.x < Constants.MAX_LAT_COORD)
				&& (this.y >= Constants.MIN_LONG_COORD) && (this.y < Constants.MAX_LONG_COORD)
				&& (this.z >= Constants.MIN_DAY_COORD) && (this.z < Constants.MAX_DAY_COORD));
	}

	public String getCoordinateKey() {
		StringBuilder sb = new StringBuilder ();
		sb.append (Integer.toString(x)).append (" ");
		sb.append (Integer.toString(y)).append (" ");
		sb.append (Integer.toString(z));
		return sb.toString ();
	}

}
