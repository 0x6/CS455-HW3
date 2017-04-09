package cs455.hadoop.Q3;

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

public class Q3Job {
    public static class Q3Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 1) {
                String state = value.toString().substring(8, 10);

                long population = populationInRange(value.toString(), 3864, 62);

                long mTotal = populationInRange(value.toString(), 3864, 13);
                long fTotal = populationInRange(value.toString(), 4143, 13);

                context.write(new Text(state + " M[0-18]"), new PairWritable(population, mTotal));
                context.write(new Text(state + " F[0-18]"), new PairWritable(population, fTotal));

                mTotal = populationInRange(value.toString(), 3981, 5);
                fTotal = populationInRange(value.toString(), 4260, 5);

                context.write(new Text(state + " M[19-29]"), new PairWritable(population, mTotal));
                context.write(new Text(state + " F[19-29]"), new PairWritable(population, fTotal));

                mTotal = populationInRange(value.toString(), 4026, 2);
                fTotal = populationInRange(value.toString(), 4305, 2);

                context.write(new Text(state + " M[30-39]"), new PairWritable(population, mTotal));
                context.write(new Text(state + " F[30-39]"), new PairWritable(population, fTotal));
            }
        }

        protected long populationInRange(String value, int start, int it){
            long total = 0;

            for(int i = 0; i < it; i++){
                total += new Long(value.substring(start + (i*9), start + ((i+1) * 9)));

            }
            return total;
        }
    }

    public static class Q3Reducer extends Reducer<Text, PairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            long agePopulation = 0;
            long population = 0;

            for(PairWritable val : values){
                population += val.getFirst();
                agePopulation += val.getSecond();
            }

            context.write(key, new Text((double)agePopulation/population + ""));
        }
    }

    public static class PairWritable implements Writable {
        private long first;
        private long second;

        public PairWritable() {
            set(first, second);
        }
        public PairWritable(long first, long second) {
            set(first, second);
        }
        public void set(long first, long second) {
            this.first = first;
            this.second = second;
        }
        public long getFirst() {
            return first;
        }
        public long getSecond() {
            return second;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(first);
            out.writeLong(second);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readLong();
            second = in.readLong();
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            long temp;
            temp = Double.doubleToLongBits(first);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(second);
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
            if (Double.doubleToLongBits(first) != Double
                    .doubleToLongBits(other.first)) {
                return false;
            }
            if (Double.doubleToLongBits(second) != Double
                    .doubleToLongBits(other.second)) {
                return false;
            }
            return true;
        }
        @Override
        public String toString() {
            return first + "," + second;
        }
    }


    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q1");

            job.setJarByClass(Q3Job.class);

            job.setMapperClass(Q3Mapper.class);
            //job.setCombinerClass(Q4Reducer.class);
            job.setReducerClass(Q3Reducer.class);

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
