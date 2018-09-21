package WordCount;

 

//just some imports that are needed. We will add these dependencies at compile time

import java.io.IOException;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

 

public class WordCount extends Configured implements Tool {

 

   //main method to kick of the run method

   public static void main(String[] args) throws Exception{

       int exitCode = ToolRunner.run(new WordCount(), args);

       System.exit(exitCode);

 

   }

 

   //housekeeping to ensure that the MapReduce job knows what is going to happen

   public int run(String[] args) throws Exception{

       Job job = Job.getInstance(getConf(), "wordcount");

       job.setJarByClass(this.getClass());

       FileInputFormat.addInputPath(job, new Path(args[0]));

       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       job.setMapperClass(Map.class);

       job.setReducerClass(Reduce.class);

       job.setOutputKeyClass(Text.class);

       job.setOutputValueClass(IntWritable.class);

 

       return job.waitForCompletion(true) ? 0 : 1;

   }

 

   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

       private final static IntWritable one = new IntWritable(1);

       private Text word = new Text();

       

       //simple regex to ensure that words get seperated

       private static final Pattern WORD_CRITERIA = Pattern.compile("\\s*\\b\\s*");

 

       public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException{

           String line = lineText.toString();

           Text currentWord = new Text();

           

           //maps every word to a key value of <word, 1> and passes it to the context to be ready for //the reduce phase

           for(String word: WORD_CRITERIA.split(line.toLowerCase())){

               if (word.isEmpty()) {

                   continue;

 

               }

               currentWord = new Text(word);

               context.write(currentWord,one);

 

           }

 

       }

   }

 

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

       @Override

       public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException{

          

         //sets the sum of each word to 0 and then counts each word and puts the value into a new key //value pair. E.g <word, 3>  

         int sum = 0;

           for (IntWritable count : counts) {

               sum += count.get();

           }

           context.write(word, new IntWritable(sum));

       }

 

   }

}