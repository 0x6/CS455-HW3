package cs455.hadoop.Q5;

import cs455.hadoop.Q1.Q1Job;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Q5Job {
    private static int total = 0;

    public static class Q5Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 2) {
                String state = value.toString().substring(8, 10);

                context.write(new Text(state + " <15k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2928, 2937))));
                context.write(new Text(state + " 15k-20k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2937, 2946))));
                context.write(new Text(state + " 20k-25k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2946, 2955))));
                context.write(new Text(state + " 25k-30k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2955, 2964))));
                context.write(new Text(state + " 30k-35k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2964, 2973))));
                context.write(new Text(state + " 35k-40k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2973, 2982))));
                context.write(new Text(state + " 40k-45k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2982, 2991))));
                context.write(new Text(state + " 45k-50k"), new PairWritable(new Text(""), new Integer(value.toString().substring(2991, 3000))));
                context.write(new Text(state + " 50k-60k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3000, 3009))));
                context.write(new Text(state + " 60k-75k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3009, 3018))));
                context.write(new Text(state + " 75k-100k"),new PairWritable(new Text(""), new Integer(value.toString().substring(3018, 3027))));

                context.write(new Text(state + " 100k-125k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3027, 3036))));
                context.write(new Text(state + " 125k-150k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3036, 3045))));
                context.write(new Text(state + " 150k-175k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3045, 3054))));
                context.write(new Text(state + " 175k-200k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3054, 3063))));
                context.write(new Text(state + " 200k-250k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3063, 3072))));
                context.write(new Text(state + " 250k-300k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3072, 3081))));
                context.write(new Text(state + " 300k-400k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3081, 3090))));
                context.write(new Text(state + " 400k-500k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3090, 3099))));
                context.write(new Text(state + " >500k"), new PairWritable(new Text(""), new Integer(value.toString().substring(3099, 3108))));
            }
        }
    }

    public static class Q5Combiner extends Reducer<Text, PairWritable, Text, PairWritable>{
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String state = "";

            for(PairWritable val: values){
                count += val.getInt();
            }

            context.write(new Text(key.toString().substring(0, 2) + " Median"), new PairWritable(new Text(key.toString().substring(3)), count));
        }
    }

    public static class Q5Reducer extends Reducer<Text, PairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<String>();
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            total = 0;

            for(PairWritable val: values){
                total += val.getInt();

                if(!map.keySet().contains(val.getText().toString())) {
                    list.add(val.getText().toString());
                    map.put(val.getText().toString(), new Integer(val.getInt()));
                }
                else{
                    int temp = map.get(val.getText().toString());
                    temp += val.getInt();

                    map.put(val.getText().toString(), new Integer(temp));
                }

            }

            java.util.Collections.sort(list, new ValueComparator());

            int count = 0;
            for(String str: list){
                count += map.get(str);

                if(count > (total+1)/2){
                    context.write(key, new Text(str));
                    break;
                }
            }
        }

        static class ValueComparator implements Comparator<String>{
            public int compare(String first, String second){
                if(first.contains("<"))
                    return -1;
                if(first.contains(">"))
                    return 1;
                if(second.contains("<"))
                    return 1;
                if(second.contains(">"))
                    return -1;

                Pattern p = Pattern.compile("[0-9]+");
                Matcher m = p.matcher(first);

                int firstNum = 0;
                int secondNum = 0;
                if(m.find())
                    firstNum = new Integer(m.group());

                m = p.matcher(second);

                if(m.find())
                    secondNum = new Integer(m.group());

                if(firstNum < secondNum)
                    return -1;
                else if(firstNum > secondNum)
                    return 1;

                return 0;
            }
        }
    }

    public static class PairWritable implements Writable{
        private Text first;
        private int second;

        public PairWritable(){
            set(new Text(""), 0);
        }

        public PairWritable(Text first, int second){
            set(first, second);
        }

        public void set(Text first, int second){
            this.first = first;
            this.second = second;
        };

        public int getInt(){
            return second;
        }

        public Text getText(){
            return first;
        }

        public void write(DataOutput out) throws IOException{
            first.write(out);
            out.writeInt(second);
        }

        public void readFields(DataInput in) throws IOException{
            first.readFields(in);
            second = in.readInt();
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q5");

            job.setJarByClass(Q5Job.class);

            job.setMapperClass(Q5Mapper.class);
            job.setCombinerClass(Q5Combiner.class);
            job.setReducerClass(Q5Reducer.class);

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
