package cs455.hadoop.Q7;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Q7Job {
    public static class Q7Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 2) {

                context.write(new Text("1 Room"), new PairWritable(new Text(""), new Integer(value.toString().substring(2388, 2397))));
                context.write(new Text("2 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2397, 2406))));
                context.write(new Text("3 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2406, 2415))));
                context.write(new Text("4 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2415, 2424))));
                context.write(new Text("5 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2424, 2433))));
                context.write(new Text("6 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2433, 2442))));
                context.write(new Text("7 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2442, 2451))));
                context.write(new Text("8 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2451, 2460))));
                context.write(new Text("9 Rooms"), new PairWritable(new Text(""), new Integer(value.toString().substring(2460, 2469))));
            }
        }
    }

    public static class Q7Combiner extends Reducer<Text, PairWritable, Text, PairWritable>{
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PairWritable val: values) {
                count += val.getInt();
            }

            context.write(new Text("Rooms"), new PairWritable(new Text(key.toString()), count));
        }
    }

    public static class Q7Reducer extends Reducer<Text, PairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<String>();
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            int total = 0;

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
            total *= .95;

            for(String str: list){
                count += map.get(str);

                //context.write(new Text("Data"), new Text(str + " " + map.get(str)));
                if(count > total){
                    context.write(new Text("95 Percentile"), new Text(str));
                    break;
                }
            }
        }

        static class ValueComparator implements Comparator<String>{
            public int compare(String first, String second){
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

            job.setJarByClass(Q7Job.class);

            job.setMapperClass(Q7Mapper.class);
            job.setCombinerClass(Q7Combiner.class);
            job.setReducerClass(Q7Reducer.class);

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
