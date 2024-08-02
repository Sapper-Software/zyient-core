/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.persistence.impl.mail;

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
