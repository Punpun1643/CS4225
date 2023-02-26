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

  // overview: 
  /*
   * What does it do: First, read in the stopwords from HDFS and store it. 
   * It then iterate through all the words from the input chunk. 
   * It then output only the words that satisfy these condition that: the word must have length greater than 4 and it must not be one of the stopwords.
   */

  // input key is of type Object = representing the input chunk, input value is of type Text = representing text in that input chunk
  // output key is of type Text = representing the word, output value is of type Text = source filename of that word.
  // we output the filename since it would be used in the reducer to check if a particular word appear in both input files. 
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    // initialize mapper output (key, value)
    private Text word = new Text(); 
    private Text sourceFile = new Text();

    // initialize hashset to store stopwords
    private HashSet<String> stopWords = new HashSet<>();

    @Override
    // purpose: to read and store the stopwords from the stopword file.
    protected void setup(Context con) throws IOException, InterruptedException {
      // get the configuration from context obj
      Configuration conf = con.getConfiguration();

      // retrive the path to stopword file. This set up in the driver
      String stopWordsPath = conf.get("stopwords.txt");

      // read the file content of the stopword file
      BufferedReader br = new BufferedReader(
        new FileReader(new File(stopWordsPath))
      );


      // iterate through the file line by line. as long as there is a word, 
      // store it in the hashset
      String stopWord;
      while ((stopWord = br.readLine()) != null) {
        stopWords.add(stopWord);
      }
    }
    // reason: The reason we read the stopwords in the setup method is because the method is called only once at the beginning of each map task.
    // This ensures that the stop words are only read and loaded into the memory once, instead of being read for each input to the mapper.



    @Override
    public void map(Object key, Text value, Context con)
      throws IOException, InterruptedException {

      // initialize the iterator 
      StringTokenizer itr = new StringTokenizer(value.toString());

      // while we still have yet to read all the word
      while (itr.hasMoreTokens()) {
        // we get the next word or token
        String token = itr.nextToken();
        // get the sourcefile name of that word, by getting the input split and obtain the 
        // source file name of that input split
        String filePath = ((FileSplit) con.getInputSplit()).getPath().getName();

        // length greater than 4, and is not stop word
        if (token.length() > 4 && !stopWords.contains(token)) {
          word.set(token);
          sourceFile.set(filePath);

          // output the word and sourcefile name of that word
          con.write(word, sourceFile);
        }
      }
    }
  }

  /*
   * overview
   * Takes in input from the mapper, 
   * check if a particular word appear in both file and also obtain the frequency that that word appear in each of the file. 
   * If the word appear in both file, the frequency of the word would be the lowest frequency from either file.
   * The reducer has a treemap which keep track of the frequency and the list of words with that frequency. 
   * Stored in the descending order of that frequency. 
   */

  // input: key value pair output from the mapper 
  // output: key of type intwritable representing the word frequency, value of type text, representing the word.
  public static class CommonWordsReducer
    extends Reducer<Text, Text, IntWritable, Text> {

      // initialize the output key
    private IntWritable wordCount = new IntWritable();
    // intialize treemap, sort the frequency in descending order
    private Map<Integer, ArrayList<Text>> freqMap = new TreeMap<>(
      Collections.reverseOrder()
    );

    @Override
    public void reduce(Text key, Iterable<Text> values, Context con)
      throws IOException, InterruptedException {

        // initialize variables to keep track whether a word appear in first and second input file.
      boolean input1 = false;
      boolean input2 = false;

      // initalize the frequency counter of a word in each input file.
      Integer sum1 = 0;
      Integer sum2 = 0;

      // retrive the path to both input file, set in the driver
      Configuration conf = con.getConfiguration();
      String filePath1 = conf.get("task1-input1.txt");
      String filePath2 = conf.get("task1-input2.txt");

        // split the path, to obtain the source filename later on
      String[] elem1 = filePath1.split("/");
      String[] elem2 = filePath2.split("/");

      // iterate through all the sourcefile name for each word.
      for (Text value : values) {
        if (value.toString().equals(elem1[elem1.length - 1])) {
          // if the source file is the first input file set input1 to true and increment the frequency
          input1 = true;
          sum1++;
        } else if (value.toString().equals(elem2[elem2.length - 1])) {
          // if the source file is the second input file set input1 to true and increment the frequency
          input2 = true;
          sum2++;
        }
      }

      // if the word appear in both file, get the lowest frequency from either file. that would be the freq of the word
      if (input1 && input2) {
        Integer minCount = Math.min(sum1.intValue(), sum2.intValue());

        // store the word in the arraylist of that frequency.
        if (!freqMap.containsKey(minCount)) {
          // if the freq does not exist yet in the freqmap, initlize the array list
          freqMap.put(minCount, new ArrayList<>());
        }

        // initialize the word 
        Text word = new Text(key.toString());

        // then store it in the array list of that frequency, else if that frequency already appear we skip 
        // the array list initalization step
        freqMap.get(minCount).add(word); 

      }
    }

    @Override
    // call once after all the input split are processed. 
    // this is to make sure that the freq ranking is correct.
    protected void cleanup(Context con)
      throws IOException, InterruptedException {

      Configuration conf = con.getConfiguration();
      // retrive the value k
      Integer k = Integer.parseInt(conf.get("k"));

      // count how many words the reducer have output
      int i = 0;

      // iterate through the freqwency in descenign order
      for (Integer freq : freqMap.keySet()) {

        // if the number of output pairs reach the top k, break
        if (i == k) {
          break;
        }

        // get the array list of that frequency 
        List<Text> words = freqMap.get(freq);
        // sort the arraylist in lexicographical order 
        Collections.sort(words);


        // iterate through the word in the arraylist and output the frequency and word
        // as key value pair so long as the counter i has not reach k
        for (Text word : words) {

          // break anytime if i reach k 
          if (i == k) {
            break;
          }

          con.write(new IntWritable(freq), word);
          // increment the counter 
          i++;
        }
      }
    }
  }

  // driver
  public static void main(String[] args) throws Exception {
    // intialize the config object
    Configuration conf = new Configuration();

    // initalize the parameters so that it could be retrive inside the mapper and reducer.
    Path stopWords = new Path(args[2]);
    Path inputPath1 = new Path(args[0]);
    Path inputPath2 = new Path(args[1]);
    String k = args[4];
    conf.set("stopwords.txt", stopWords.toString());
    conf.set("task1-input1.txt", inputPath1.toString());
    conf.set("task1-input2.txt", inputPath2.toString());
    conf.set("k", k);

    // set up the input and output classes for the mapper and reducer
    Job job = Job.getInstance(conf, "top k words");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CommonWordsReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    // set input file path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));

    // set otuput file path
    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    // exit once the job is done.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
