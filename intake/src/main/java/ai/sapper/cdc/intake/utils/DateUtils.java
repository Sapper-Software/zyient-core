package ai.sapper.cdc.intake.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	public static final String FORMAT_PREFIX = "fmt#";
	public static final String DEFAULT_DATE_FORMAT = "MM/dd/yyyy HH:mm:SS";
	public static final String DEFAULT_TIMESTAMP_FORMAT = "MMddyyyyHHmmSS";

	public static Date parse(@Nonnull String input, @Nonnull String format) throws ParseException {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(input));
		Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

		input = input.trim();
		if (StringUtils.isNumeric(input)) {
			return new Date(Long.parseLong(input));
		}
		int fidx = input.indexOf(FORMAT_PREFIX);
		String dates = input;
		if (fidx > 0) {
			dates = input.substring(0, fidx);
			format = input.substring(fidx + FORMAT_PREFIX.length());
		}
		SimpleDateFormat df = new SimpleDateFormat(format);
		return df.parse(dates);
	}

	public static Date parse(@Nonnull String input) throws ParseException {
		return parse(input, DEFAULT_DATE_FORMAT);
	}

	public static String formatTimestamp(long timestamp, String format) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

		SimpleDateFormat df = new SimpleDateFormat(format);
		return df.format(new Date(timestamp));
	}

	public static String formatTimestamp(String format) {
		return formatTimestamp(System.currentTimeMillis(), format);
	}

	public static String formatTimestamp() {
		return formatTimestamp(DEFAULT_TIMESTAMP_FORMAT);
	}
}
