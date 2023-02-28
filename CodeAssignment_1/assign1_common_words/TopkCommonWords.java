/*
NAME: Papattarada Apithanangsiri
MATRICULATION NUMBER: A0222677Y
*/

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

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

    private HashSet<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context con) throws IOException, InterruptedException {
      Configuration conf = con.getConfiguration();
      String stopWordsPath = conf.get("stopwords");

      BufferedReader br = new BufferedReader(
        new FileReader(new File(stopWordsPath))
      );

      String stopWord;
      while ((stopWord = br.readLine()) != null) {
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
    extends Reducer<Text, Text, IntWritable, Text> {
    private IntWritable wordCount = new IntWritable();
    private Map<Integer, ArrayList<Text>> freqMap = new TreeMap<>(
      Collections.reverseOrder()
    );

    @Override
    public void reduce(Text key, Iterable<Text> values, Context con)
      throws IOException, InterruptedException {
      boolean input1 = false;
      boolean input2 = false;
      Integer sum1 = 0;
      Integer sum2 = 0;
      Configuration conf = con.getConfiguration();
      String filePath1 = conf.get("input1");
      String filePath2 = conf.get("input2");
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
        Integer minCount = Math.min(sum1.intValue(), sum2.intValue());

        // store in key: freq, value: words with that freq
        if (!freqMap.containsKey(minCount)) {
          freqMap.put(minCount, new ArrayList<>());
        }

        Text word = new Text(key.toString());
        freqMap.get(minCount).add(word); 

      }
    }

    @Override
    protected void cleanup(Context con)
      throws IOException, InterruptedException {

      Configuration conf = con.getConfiguration();
      Integer k = Integer.parseInt(conf.get("k"));

      int i = 0;
      for (Integer freq : freqMap.keySet()) {
        if (i == k) {
          break;
        }

        List<Text> words = freqMap.get(freq);
        Collections.sort(words);

        for (Text word : words) {
          if (i == k) {
            break;
          }

          con.write(new IntWritable(freq), word);
          i++;
        }
      }
    }
  }

  // driver
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Path stopWords = new Path(args[2]);
    Path inputPath1 = new Path(args[0]);
    Path inputPath2 = new Path(args[1]);
    String k = args[4];
    conf.set("stopwords", stopWords.toString());
    conf.set("input1", inputPath1.toString());
    conf.set("input2", inputPath2.toString());
    conf.set("k", k);

    Job job = Job.getInstance(conf, "top k words");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CommonWordsReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));

    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
