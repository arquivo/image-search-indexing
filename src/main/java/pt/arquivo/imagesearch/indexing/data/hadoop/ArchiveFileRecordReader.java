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


public class ArchiveFileRecordReader extends RecordReader<LongWritable, WritableArchiveRecord> {

    private static Logger log = Logger.getLogger(ArchiveFileRecordReader.class
            .getName());

    private FileSplit fileSplit;
    private Configuration conf;

    private FSDataInputStream datainputstream;

    private FileStatus status;
    private FileSystem fs;

    private ArchiveReader arcreader;
    private Iterator<ArchiveRecord> iterator;
    private ArchiveRecord record;
    private String archiveName;

    public long getPos() throws IOException {
        try {
            datainputstream.available();
        } catch (Exception e) {
            return 0;
        }
        return datainputstream.getPos();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();

        Path file = fileSplit.getPath();
        this.fs = file.getFileSystem(conf);

        this.status = this.fs.getFileStatus(file);
        this.datainputstream = this.fs.open(file);

        this.arcreader = ArchiveReaderFactory.get(file.getName(), datainputstream, true);
        this.arcreader.setStrict(true);
        this.iterator = arcreader.iterator();
        this.archiveName = arcreader.getFileName();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(getPos());
    }

    @Override
    public WritableArchiveRecord getCurrentValue() throws IOException, InterruptedException {
        return new WritableArchiveRecord(this.record);
    }

    @Override
    public float getProgress() throws IOException {
        if (datainputstream != null && this.status != null) {
            return (float) datainputstream.getPos()
                    / (float) this.status.getLen();
        } else {
            return 1.0f;
        }
    }

    @Override
    public void close() throws IOException {
        if (datainputstream != null) {
            try {
                datainputstream.close();
            } catch (IOException e) {
                log.error("close(): " + e.getMessage());
            }
        }
    }
}

