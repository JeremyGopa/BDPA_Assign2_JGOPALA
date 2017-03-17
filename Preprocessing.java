package edu.stanford.cs246.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class prepro extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new prepro(), args);
      
      System.exit(res);
   }

   	
   public static enum COUNTER { //create a counter for number of lines in output
	   LINES,
	 };
	 
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "prepro");
      job.setJarByClass(prepro.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
      job.waitForCompletion(true);
      File file = new File("/home/cloudera/workspace/lines_output.txt"); //new file to store the counter output
      file.createNewFile(); // create the file
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      long lines = job.getCounters().findCounter(COUNTER.LINES).getValue(); //store the value of counter to "lines"
      writer.write(Long.toString(lines)); //write lines to the file
      writer.flush(); //flush to make sure data from buffer is writtent before closing
      writer.close(); //close the writer
      return 0;
   }

   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  //putting stopwords into a list thanks to scanner
    	  Scanner s = new Scanner(new File("/home/cloudera/workspace/stopwords.txt"));
    	  ArrayList<String> list = new ArrayList<String>();
    	  while (s.hasNext()){
    	      list.add(s.next());
    	  }
    	  s.close(); 
		  		
    	  StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'#--()_\"*/$%&<>+=@"); //remove all the characters indicated and separating words
    	  while (tokenizer.hasMoreTokens()) { 
    		  String stri = tokenizer.nextToken(); //put words as strings
    			if (list.contains(stri)) { //check if word is in stopwords list or not
    				
    			} else {
    			context.write(key, new Text(stri)); //if not, add the key(byte offset of line beginning) and value(word) to mapper output
    			}
         }
      }
   }

   public static class Reduce extends Reducer<LongWritable, Text, Long, Text> {
	    
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> sum = new ArrayList<String>(); //create an Hashset in which we will put unique words of each line

			for (Text value : values) {
					sum.add(value.toString()); //put the words into the Hashset as strings
				
			}
			HashSet<String> sum1 = new HashSet<String>(sum);
			
			HashMap<String, String> map = new HashMap<String, String>(); //new Hashmap to put global frequency of each word
			BufferedReader reader = new BufferedReader( new FileReader("/home/cloudera/workspace/wordcount.txt"));
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(","); //split wordcount into key(word), value(count)
				map.put(parts[0], parts[1]); // put them into Hashmap
				
			}
			reader.close();
			
			//put the key/values of the previous Hashmap into a list of pairs.
			List<Pair<String, Integer>> words = new ArrayList<Pair<String, Integer>>();
			for (String value : sum1) {
				if (map.get(value) == null){
					words.add(new Pair<String, Integer>(value,1)); //if the word does not appear in wordcount
				}
				else
				{
					words.add(new Pair<String, Integer>(value,Integer.parseInt(map.get(value)))); 
				}
			
			}
			
			
			// order the pairs according to their frequency. Ascending order
			Collections.sort(words, new Comparator<Pair<String, Integer>>() {
			    @Override
			    public int compare(final Pair<String, Integer> o1, final Pair<String, Integer> o2) {
					return Integer.compare(o1.getSecond(),o2.getSecond());
			        
			    }
			});
			
			
			StringBuilder Stringbuilder = new StringBuilder();
			boolean isfirst = true; //initial value
			for (int i = 0; i<words.size();i++) { //i<words.size() because we start from 0
				if (!isfirst){
				Stringbuilder.append(" "); //if not first value, add a space as separator
				}
				isfirst = false; 
				Stringbuilder.append((words.get(i)).getFirst()); //append the word to the Stringbuilder
			}
			//output the line number as key and the words ordered by frequency as value
			long nbline = context.getCounter(COUNTER.LINES).getValue()+1; //get the current line number
			context.write(nbline,new Text (Stringbuilder.toString()));
			context.getCounter(COUNTER.LINES).increment(1); //increment our counter
			
		}
	}
}
