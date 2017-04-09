package cs455.hadoop.Q6;

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

public class Q6Job {
    public static class Q6Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (new Integer(value.toString().substring(24, 28)) == 2) {
                String state = value.toString().substring(8, 10);

                context.write(new Text(state + " <100"), new PairWritable(new Text(""), new Integer(value.toString().substring(3450, 3459))));
                context.write(new Text(state + " 100-150"), new PairWritable(new Text(""), new Integer(value.toString().substring(3459, 3468))));
                context.write(new Text(state + " 150-200"), new PairWritable(new Text(""), new Integer(value.toString().substring(3468, 3477))));
                context.write(new Text(state + " 200-250"), new PairWritable(new Text(""), new Integer(value.toString().substring(3477, 3486))));
                context.write(new Text(state + " 250-300"), new PairWritable(new Text(""), new Integer(value.toString().substring(3486, 3495))));
                context.write(new Text(state + " 300-350"), new PairWritable(new Text(""), new Integer(value.toString().substring(3495, 3504))));
                context.write(new Text(state + " 350-400"), new PairWritable(new Text(""), new Integer(value.toString().substring(3504, 3513))));
                context.write(new Text(state + " 400-450"), new PairWritable(new Text(""), new Integer(value.toString().substring(3513, 3522))));
                context.write(new Text(state + " 450-500"), new PairWritable(new Text(""), new Integer(value.toString().substring(3522, 3531))));
                context.write(new Text(state + " 500-550"), new PairWritable(new Text(""), new Integer(value.toString().substring(3531, 3540))));
                context.write(new Text(state + " 550-600"),new PairWritable(new Text(""), new Integer(value.toString().substring(3540, 3549))));
                context.write(new Text(state + " 600-650"), new PairWritable(new Text(""), new Integer(value.toString().substring(3549, 3558))));
                context.write(new Text(state + " 650-700"), new PairWritable(new Text(""), new Integer(value.toString().substring(3558, 3567))));
                context.write(new Text(state + " 700-750"), new PairWritable(new Text(""), new Integer(value.toString().substring(3567, 3576))));
                context.write(new Text(state + " 750-1000"), new PairWritable(new Text(""), new Integer(value.toString().substring(3576, 3585))));
                context.write(new Text(state + " >1000"), new PairWritable(new Text(""), new Integer(value.toString().substring(3585, 3594))));
                context.write(new Text(state + " No cash rent"), new PairWritable(new Text(""), new Integer(value.toString().substring(3594, 3603))));
            }
        }
    }

    public static class Q6Combiner extends Reducer<Text, PairWritable, Text, PairWritable>{
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PairWritable val: values) {
                count += val.getInt();
            }

            context.write(new Text(key.toString().substring(0, 2) + " Median"), new PairWritable(new Text(key.toString().substring(3)), count));
        }
    }

    public static class Q6Reducer extends Reducer<Text, PairWritable, Text, Text> {
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
            for(String str: list){
                count += map.get(str);

                //context.write(new Text("Data"), new Text(str + " " + map.get(str)));
                if(count > total/2){
                    context.write(key, new Text(str));
                    break;
                }
            }
        }

        static class ValueComparator implements Comparator<String>{
            public int compare(String first, String second){
                if(first.contains("No cash rent"))
                    return -1;
                if(second.contains("No cash rent"))
                    return 1;
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

            job.setJarByClass(Q6Job.class);

            job.setMapperClass(Q6Mapper.class);
            job.setCombinerClass(Q6Combiner.class);
            job.setReducerClass(Q6Reducer.class);

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
