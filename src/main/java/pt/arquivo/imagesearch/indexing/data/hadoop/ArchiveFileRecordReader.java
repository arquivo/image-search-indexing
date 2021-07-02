package pt.arquivo.imagesearch.indexing.data.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.util.Iterator;


/**
 * This class enables iterating the (W)ARC records read from HDFS
 * Used in the HDFSImageIndexerWithDupsJob clause
 */
public class ArchiveFileRecordReader extends RecordReader<LongWritable, WritableArchiveRecord> {

    private static final Logger log = Logger.getLogger(ArchiveFileRecordReader.class
            .getName());


    /**
     * Input stream for the (W)ARC file
     */
    private FSDataInputStream datainputstream;


    /**
     * Current (W)ARC file status
     */
    private FileStatus status;

    /**
     * (W)ARC record iterador
     */
    private Iterator<ArchiveRecord> iterator;

    /**
     * Current record that is being processed
     */
    private ArchiveRecord record;

    /**
     * (W)ARC name
     */
    private String archiveName;

    public long getPos() throws IOException {
        try {
            datainputstream.available();
        } catch (Exception e) {
            return 0;
        }
        return datainputstream.getPos();
    }

    /**
     * Prepares data for being iterated by record
     *
     * @param split (W)ARC file in HDFS
     * @param context Hadoop context
     * @throws IOException error reading data from HDFS
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();

        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);

        this.status = fs.getFileStatus(file);
        this.datainputstream = fs.open(file);

        ArchiveReader arcreader = ArchiveReaderFactory.get(file.getName(), datainputstream, true);
        arcreader.setStrict(true);
        this.iterator = arcreader.iterator();
        this.archiveName = arcreader.getFileName();
    }

    /**
     * Whether the current record being iterated has more records
     *
     * @return whether the (W)ARC file has more records
     */
    @Override
    public boolean nextKeyValue() {
        boolean hasNext = false;
        try {
            hasNext = iterator.hasNext();
            if (hasNext) {
                this.record = iterator.next();
            }
        } catch (Throwable e) {
            log.error("ERROR in hasNext():  " + this.archiveName + ": "
                    + e.toString());
        }
        return hasNext;
    }

    /**
     * Gets current interator key
     *
     * @return key for the current record
     * @throws IOException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException {
        return new LongWritable(getPos());
    }

    /**
     * Get the current value for the iterator
     *
     * @return current record object
     * @throws IOException
     */
    @Override
    public WritableArchiveRecord getCurrentValue() throws IOException {
        return new WritableArchiveRecord(this.record);
    }

    /**
     * Gets the progress in the current file
     *
     * @return position in the stream as a ratio between 0 and 1
     * @throws IOException
     */
    @Override
    public float getProgress() throws IOException {
        if (datainputstream != null && this.status != null) {
            return (float) datainputstream.getPos()
                    / (float) this.status.getLen();
        } else {
            return 1.0f;
        }
    }

    /**
     * Closes the current stream
     */
    @Override
    public void close() {
        if (datainputstream != null) {
            try {
                datainputstream.close();
            } catch (IOException e) {
                log.error("close(): " + e.getMessage());
            }
        }
    }
}

