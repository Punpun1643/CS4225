/*
NAME: Papattarada Apithanangsiri
MATRICULATION NUMBER: A0222677Y
*/

import java.io.*;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

  // mapper: tokenize words from file
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text sourceFile = new Text();

    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context con) throws IOException, InterruptedException {
      Configuration conf = con.getConfiguration();
      String stopWordsPath = conf.get("stopwords.txt");

      BufferedReader br = new BufferedReader(
        new FileReader(new File(stopWordsPath))
      );
      while (br.readLine() != null) {
        String stopWord = br.readLine();
        stopWords.add(stopWord);
      }
    }

    @Override
    public void map(Object key, Text value, Context con)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        String filePath = ((FileSplit) con.getInputSplit()).getPath().getName();

        // length greater than 4, and is not stop word
        if (token.length() > 4 && !stopWords.contains(token)) {
          word.set(token);
          sourceFile.set(filePath);

          con.write(word, sourceFile);
        }
      }
    }
  }

  // reducer: count each word
  public static class CommonWordsReducer
    extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable wordCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context con)
      throws IOException, InterruptedException {
      boolean input1 = false;
      boolean input2 = false;
      int sum1 = 0;
      int sum2 = 0;
      Configuration conf = con.getConfiguration();
      String filePath1 = conf.get("task1-input1.txt");
      String filePath2 = conf.get("task1-input2.txt");
      String[] elem1 = filePath1.split("/");
      String[] elem2 = filePath2.split("/");

      for (Text value : values) {
        if (value.toString().equals(elem1[elem1.length - 1])) { 
          input1 = true;
          sum1++;
        } else if (value.toString().equals(elem2[elem2.length - 1])) { 
          input2 = true;
          sum2++;
        }
      }

      if (input1 && input2) {
        System.out.println("Enter this loop");
        int minCount = Math.min(sum1, sum2);
        con.write(key, new IntWritable(minCount));
      }
    }
  }

  // driver
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Path stopWords = new Path(args[2]);
    Path inputPath1 = new Path(args[0]);
    Path inputPath2 = new Path(args[1]);
    conf.set("stopwords.txt", stopWords.toString());
    conf.set("task1-input1.txt", inputPath1.toString());
    conf.set("task1-input2.txt", inputPath2.toString());

    Job job = Job.getInstance(conf, "top k words");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CommonWordsReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));

    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
