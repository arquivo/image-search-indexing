import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

import com.mongodb.ServerAddress;

/**
 * Utility methos
 *
 */
public class ImageSearchIndexingUtil {

	private static Logger logger = Logger.getLogger(ImageSearchIndexingUtil.class);

	public static void readArcRecords(String arcURL, Consumer<ARCRecord> consumer) {
		logger.debug("Reading ARC records for: " + arcURL);
		ARCReader reader;
		try {
			reader = ARCReaderFactory.get(arcURL);
		} catch (Exception e) {
			logger.error("Exception starting reading ARC", e);
			return;
		}

		int records = 0;
		int errors = 0;
		for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
			ARCRecord record;
			try {
				record = (ARCRecord) ii.next();
			} catch (RuntimeException e) {
				errors++;
				// skip this record
				logger.error("Exception reading next (W)ARC record", e);
				break;
			}
			try {
				consumer.accept(record);
			} catch (RuntimeException e) {
				logger.error("Exception reading (W)ARC record", e);
				errors++;
			}

			++records;
			if (record.hasErrors()) {
				errors += record.getErrors().size();
			}
		}
		logger.debug("records: " + records);
		logger.debug("errors: " + errors);
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				logger.debug("error closing ArchiveReader" + e.getMessage());
			}
		}
	}

	public static void readWarcRecords(String warcURL, Consumer<WARCRecordResponseEncapsulated> consumer) {
		logger.debug("Reading WARC records for: " + warcURL);
		ArchiveReader reader = null;
		try {
			reader = WARCReaderFactory.get(warcURL);
		} catch (Exception e) {
			logger.error("Exception starting reading WARC", e);
			return;
		}
		int records = 0;
		int errors = 0;

		for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {

			WARCRecord warcRecord;
			try {
				warcRecord = (WARCRecord) ii.next();
			} catch (RuntimeException re) {
				errors++;
				// skip this record
				logger.error("Exception reading next WARC record", re);
				break;
			}

			WARCRecordResponseEncapsulated record = null;
			try {
				record = new WARCRecordResponseEncapsulated(warcRecord);
				consumer.accept(record);
			} catch (InvalidWARCResponseIOException e) {
				/* This is not a WARCResponse; skip */
				errors++;
			} catch (IOException e) {
				logger.debug("IO Exception reading WARCrecord WARCNAME: " + warcURL + " " + e.getMessage());
				errors++;
			} catch (Exception e) {
				logger.debug("Exception reading WARCrecord WARCNAME: " + warcURL + " " + e.getMessage());
				errors++;
			}
			++records;
			if (record != null && record.hasErrors()) {
				errors += record.getErrors().size();
			}
		}
		logger.debug("records: " + records);
		logger.debug("errors: " + errors);
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				logger.debug("error closing ArchiveReader" + e.getMessage());
			}
		}

	}

	public static List<ServerAddress> getMongoDBServerAddresses(String mongodbServers) {
		System.out.println("Using mongodb servers: " + mongodbServers);

		return Arrays.asList(mongodbServers.split(",")).stream().map(mongoServerStr -> {
			String[] ms = mongoServerStr.split(":");
			String server = ms[0];
			Integer port = Integer.valueOf(ms[1]);
			return new ServerAddress(server, port);
		}).collect(Collectors.toList());
	}

	public static byte[] getRecordContentBytes(ARCRecord record) throws IOException {
		record.skipHttpHeader();/*Skipping http headers to only get the content bytes*/
		byte[] buffer = new byte[1024 * 16];
		int len = record.read(buffer, 0, buffer.length);
		ByteArrayOutputStream contentBuffer =
				new ByteArrayOutputStream(1024 * 16* 1000); /*Max record size: 16Mb*/
		contentBuffer.reset();
		while (len != -1)
		{
			contentBuffer.write(buffer, 0, len);
			len = record.read(buffer, 0, buffer.length);
		}
		record.close();
		return contentBuffer.toByteArray();
	}

}
