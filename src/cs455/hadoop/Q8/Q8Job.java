package cs455.hadoop.Q8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class Q8Job {
    public static class Q8Mapper extends Mapper<LongWritable, Text, Text, DataWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 1) {
                String state = value.toString().substring(8, 10);

                int population = new Integer(value.toString().substring(300, 309));
                int old = new Integer(value.toString().substring(1065, 1074));

                context.write(new Text(state), new DataWritable(new Text(state), population, old));
            }
        }
    }

    public static class Q8Combiner extends Reducer<Text, DataWritable, Text, DataWritable> {
        @Override
        protected void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            int elderly = 0;

            for(DataWritable val: values){
                total += val.getPopulation();
                elderly += val.getElderlyPopulation();
            }

            context.write(new Text(">85"), new DataWritable(key, total, elderly));
        }
    }

    public static class Q8Reducer extends Reducer<Text, DataWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> totalPop = new HashMap<String, Integer>();
            HashMap<String, Integer> elderlyPop = new HashMap<String, Integer>();

            for(DataWritable val: values){
                if(!totalPop.keySet().contains(val.getState())){
                    totalPop.put(val.getState().toString(), val.getPopulation());
                    elderlyPop.put(val.getState().toString(), val.getElderlyPopulation());
                } else {
                    int tempTotal = totalPop.get(val.getState()) + val.getPopulation(), tempElderly = elderlyPop.get(val.getState()) + val.getElderlyPopulation();

                    totalPop.put(val.getState().toString(), tempTotal);
                    elderlyPop.put(val.getState().toString(), tempElderly);
                }
            }

            double percentElderly = 0;
            String state = "";
            for(String str: totalPop.keySet()){
                double temp = (double)(elderlyPop.get(str)) / totalPop.get(str);

                if(temp > percentElderly){
                    percentElderly = temp;
                    state = str;
                }
            }

            context.write(new Text(state), new Text( percentElderly + ""));
        }
    }

    public static class DataWritable implements Writable{
        private Text state;
        private int population;
        private int elderly;

        public DataWritable(){
            set(new Text(""), 0, 0);
        }

        public DataWritable(Text first, int second, int third){
            set(first, second, third);
        }

        public void set(Text first, int second, int third){
            this.state = first;
            this.population = second;
            this.elderly = third;
        };

        public int getPopulation(){
            return population;
        }

        public int getElderlyPopulation(){
            return elderly;
        }

        public Text getState(){
            return state;
        }

        public void write(DataOutput out) throws IOException{
            state.write(out);
            out.writeInt(population);
            out.writeInt(elderly);
        }

        public void readFields(DataInput in) throws IOException{
            state.readFields(in);
            population = in.readInt();
            elderly = in.readInt();
        }
    }


    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q1");

            job.setJarByClass(Q8Job.class);

            job.setMapperClass(Q8Mapper.class);
            job.setCombinerClass(Q8Combiner.class);
            job.setReducerClass(Q8Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DataWritable.class);
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
