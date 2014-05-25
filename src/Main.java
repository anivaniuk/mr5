
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

    private static List<String> filter;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text title = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            word.set("123");
//            context.write(word, value);
//            System.out.println(value.toString() + "---");
            int index = value.toString().indexOf('\n');
            System.out.println(value.toString().replaceAll("\n", " "));
            String name = value.toString().substring(0, index);
            String body = value.toString().substring(index);

            for (String w : filter) {
                if (body.contains(w)) {
                    title.set(name);
                    word.set(w);
                    context.write(word, title);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder titles = new StringBuilder();
            for (Text val : values) {
                titles.append(val.toString() + '\t');
            }
            result.set(titles.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: mr5 <in> <out> <filter>");
            System.exit(2);
        }

        String filename = otherArgs[2];

        FileReader reader = new FileReader(filename);
        BufferedReader br = new BufferedReader(reader);
        filter = new ArrayList<String>();
        while (br.ready()) {
            filter.add(br.readLine().trim());
        }

        Job job = new Job(conf, "wiki filter");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
