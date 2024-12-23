package org.oracle.okafka.common.utils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimestampConverter {
	
	public static final String UTC_ZONE_OFFSET = "+00:00"; 
	
	public static long timestampInEpochMillis(String okafkaTimestamp) {
		
		String formattedTimestamp = okafkaTimestamp.substring(0, 4).toUpperCase() + okafkaTimestamp.substring(4, 6).toLowerCase()
				+ okafkaTimestamp.substring(6).toUpperCase();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yy hh.mm.ss.SSSSSS a XXX",
				java.util.Locale.ENGLISH);
		OffsetDateTime offsetDateTime = OffsetDateTime.parse(formattedTimestamp, formatter);
		Instant instant = offsetDateTime.toInstant();
		return instant.toEpochMilli();

	}
	
	public static String timestampInUTCTimeZone(long kafkaTimestamp) {
		Instant instant = Instant.ofEpochMilli(kafkaTimestamp);

		DateTimeFormatter formatter = DateTimeFormatter
				.ofPattern("dd-MMM-yy hh.mm.ss.SSSSSS a", java.util.Locale.ENGLISH).withZone(ZoneOffset.UTC);

		return formatter.format(instant) + " " + UTC_ZONE_OFFSET;
	}
}
