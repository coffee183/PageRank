import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class UnitMultiplication {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");
            //dead ends
            if (fromTo.length < 2) {
                //exception logger.debug("found dead ends : "8")
                return;
            }
            String outputKey = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to : tos) {
                context.write(new Text(outputKey), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double prCell = 0;
            List<String> transCells = new ArrayList<String>();
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transCells.add(value.toString());
                } else {
                    prCell = Double.valueOf(value.toString());
                }
            }
            for (String transCell : transCells) {
                String toId = transCell.split("=")[0];
                double prob = Double.parseDouble(transCell.split("=")[1]);
                double subPr = prob * prCell;
                context.write(new Text(toId), new Text(String.valueOf(subPr)));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(UnitMultiplication.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }
}
