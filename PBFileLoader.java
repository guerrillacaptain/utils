package com.jd.ads.jshare.pig;

import com.jd.ad.sku.protobuf.AdSkuAttributeProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lisheng3 on 2015/6/19.
 */
public class PBFileLoader extends LoadFunc {
    protected RecordReader in;
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    public PBFileLoader() {
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PBInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        in = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            AdSkuAttributeProto.AdSkuAttribute value = (AdSkuAttributeProto.AdSkuAttribute) in.getCurrentValue();
            ArrayList<Object> fields = new ArrayList<Object>();
            fields.add(value.getSkuId());
            fields.add(value.getSkuName());
            fields.add(value.getImageUrl());
            fields.add(value.getPid());
            fields.add(value.getCid1());
            fields.add(value.getCid2());
            fields.add(value.getCid());
            fields.add(value.getSkuReadme());
            fields.add(value.getWechatPrice());
            //
            //
            fields.add(value.getVid());
            fields.add(value.getBrandId());
            fields.add(value.getSendType());
            fields.add(value.getCountry());
            fields.add(value.getProvince());
            fields.add(value.getCity());
            fields.add(value.getProviderCode());
            fields.add(value.getUpdateTime());
            fields.add(value.getShopId());
            fields.add(value.getPopPid());
            fields.add(value.getCod());
            fields.add(value.getWorldwide());
            fields.add(value.getState());
            fields.add(value.getComments());
            fields.add(value.getGoodComments());

            if (value.getSkuId() >= 1000000000L) { //pop
                fields.add(2);
                fields.add(value.getVid());
            } else { //自营
                fields.add(1);
                fields.add(0l);
            }

            return tupleFactory.newTupleNoCopy(fields);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    public static class PBInputFormat extends FileInputFormat {
        public PBInputFormat() {
        }

        @Override
        public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
            @SuppressWarnings("unchecked") List<FileStatus> fileStatuses = listStatus(jobContext);
            List<InputSplit> inputSplits = new ArrayList<InputSplit>();
            for (FileStatus fileStatus : fileStatuses) {
                FileSystem fs = fileStatus.getPath().getFileSystem(jobContext.getConfiguration());
                BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
                if (blockLocations != null && blockLocations.length >= 1) {
                    inputSplits.add(new SingleFileSplit(fileStatus, blockLocations[0].getHosts()));
                }
            }
            return inputSplits;
        }

        @Override
        public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new PBRecordReader();
        }
    }

    public static class SingleFileSplit extends InputSplit implements Writable {
        private Path file;
        private long length;
        private String[] hosts;

        public SingleFileSplit() {
        }

        public SingleFileSplit(FileStatus fileStatus, String[] hosts) {
            this.file = fileStatus.getPath();
            this.length = fileStatus.getLen();
            this.hosts = hosts;
        }

        public Path getPath() {
            return this.file;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.length;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return this.hosts == null ? new String[0] : this.hosts;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            Text.writeString(dataOutput, file.toString());
            dataOutput.writeLong(length);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            file = new Path(Text.readString(dataInput));
            length = dataInput.readLong();
            hosts = null;
        }
    }

    public static class PBRecordReader extends RecordReader<Text, AdSkuAttributeProto.AdSkuAttribute> {
        private Configuration conf;
        private FSDataInputStream in;
        private final Text key = new Text("key");
        private AdSkuAttributeProto.AdSkuAttribute value;
        private long length;

        public PBRecordReader() {
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            SingleFileSplit fileSplit = (SingleFileSplit) inputSplit;
            this.conf = taskAttemptContext.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(this.conf);
            this.in = fs.open(path);
            this.length = fileSplit.getLength();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (in.available() != 0) {

                byte[] pbLenBytes = new byte[4];
                try {
                    in.readFully(pbLenBytes, 0, 4);
                } catch (EOFException e) {
                    return false;
                }
                int pbLen = (((pbLenBytes[3] & 0xff) << 24) | ((pbLenBytes[2] & 0xff) << 16) |
                        ((pbLenBytes[1] & 0xff) << 8) | (pbLenBytes[0] & 0xff));
                byte[] pbRaw = new byte[pbLen];
                try {
                    in.readFully(pbRaw, 0, pbLen);
                } catch (EOFException e) {
                    return false;
                }
                try {
                    value = AdSkuAttributeProto.AdSkuAttribute.parseFrom(pbRaw);
                } catch (Exception e) {
                    e.printStackTrace();
                    return nextKeyValue();
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public AdSkuAttributeProto.AdSkuAttribute getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return length == 0 ? 0 : (float) in.getPos() / length;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
