package cs455.hadoop.Q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountJob {
    private static int total = 0;

    public static class Q1Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 2) {
                LongWritable[] list = {new LongWritable(new Long(value.toString().substring(1803, 1812))), new LongWritable(new Long(value.toString().substring(1812, 1821)))};

                context.write(new Text("data"), new PairWritable(new Long(value.toString().substring(1803, 1812)), new Long(value.toString().substring(1812, 1821))));
            }
        }
    }

    public static class Q1Reducer extends Reducer<Text, PairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            long renters = 0;
            long owners = 0;

            for(PairWritable val : values){
                long ownerNum = val.getOwner();
                long renterNum = val.getRenter();

                owners += ownerNum;
                renters += renterNum;

                total += ownerNum + renterNum;
            }

            context.write(key, new Text("total: " + total + " owners: " + owners + " renters: " + renters));
        }
    }

    public static class PairWritable implements Writable {
        private long owner;
        private long renter;

        public PairWritable() {
            set(owner, renter);
        }
        public PairWritable(long first, long second) {
            set(first, second);
        }
        public void set(long first, long second) {
            this.owner = first;
            this.renter = second;
        }
        public long getOwner() {
            return owner;
        }
        public long getRenter() {
            return renter;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(owner);
            out.writeLong(renter);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            owner = in.readLong();
            renter = in.readLong();
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            long temp;
            temp = Double.doubleToLongBits(owner);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(renter);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof PairWritable)) {
                return false;
            }
            PairWritable other = (PairWritable) obj;
            if (Double.doubleToLongBits(owner) != Double
                    .doubleToLongBits(other.owner)) {
                return false;
            }
            if (Double.doubleToLongBits(renter) != Double
                    .doubleToLongBits(other.renter)) {
                return false;
            }
            return true;
        }
        @Override
        public String toString() {
            return owner + "," + renter;
        }
    }


    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q1");

            job.setJarByClass(Q1Job.class);

            job.setMapperClass(Q1Mapper.class);
            //job.setCombinerClass(Q4Reducer.class);
            job.setReducerClass(Q1Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PairWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
