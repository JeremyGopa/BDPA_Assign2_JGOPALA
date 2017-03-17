package edu.stanford.cs246.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class SimilarityA extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SimilarityA(), args);
      
      System.exit(res);
   }
	 
    public static enum COUNTERA {
	   LINES,
	 };
	 
	 //create a HashMap with line number as key and words as value (put input file into HashMap)
    public static HashMap<String, String> map = new HashMap<String, String>();{
			BufferedReader reader;
			try {
				reader =new BufferedReader(new FileReader("/home/cloudera/workspace/output_prepro.txt"));
				 String line = "";
					while ((line = reader.readLine()) != null) {
						String[] parts1 = line.split(","); //split the line into key and value
						map.put(parts1[0], parts1[1]);
					}
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
			
   
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "SimilarityA");
      job.setJarByClass(SimilarityA.class);
      job.setOutputKeyClass(Text.class);
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
	      File file = new File("/home/cloudera/workspace/NBComparisonA.txt");
	      file.createNewFile();
	      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
	      long lines = job.getCounters().findCounter(COUNTERA.LINES)
					.getValue();
	      writer.write(Long.toString(lines));
	      writer.flush();
	      writer.close();
	      return 0;
   }

   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  
    	  // get the value from the counter in preprocessing (number of lines in input)
    	  Scanner scanner = new Scanner(new File("/home/cloudera/workspace/lines_output.txt"));
    	  int nblines = scanner.nextInt();
    	  scanner.close();
    	  
    	  // split input into key, value
		  String[] parts = value.toString().split(",");
		  String key1 = parts[0];
		  String value1 = parts[1];
		
		  
		//write all the (n²-n)/2 possible pairs 
		if (Integer.parseInt(key1)<nblines){ //if current key is lower than nblines
		  for (int i = Integer.parseInt(key1)+1; i<=nblines; i++){ //starts from the key number and pairs it with every line after it (the lines before it are already done) 
					StringBuilder Stringbuilder = new StringBuilder();
					Stringbuilder.append(key1 +","+ i); //write the pairs separated by a comma
					context.write(new Text(Stringbuilder.toString()), new Text(value1)); //output of mapper, pair of key and value of the first of the two keys
			}
				
      }
      }
   }
   public static class Reduce extends Reducer<Text, Text, Text, Double> {
	    
	   
	   // Jaccard similarity method
	   public final double Jaccardsim (String s1, String s2){ 
		   List<String> list1 = Arrays.asList(s1.split(" ")); //split the first string into words and put them in a list
	       List<String> list2 = Arrays.asList(s2.split(" "));// split the second string into words and put them in a list
				HashSet<String> sim = new HashSet<>(list1); //use Hashset to get each unique words
				sim.addAll(list2); //add the different words from list2 to get unique words in both
				double total = list1.size()+list2.size(); //total number of words including duplicates
				double union = sim.size(); //only unique words
				double inter = total - union; //only words that appear more than once (as words can appear only once in one list this is words that appear in both lists)
			
			return inter/union; //return Jaccard Similarity
		}
	   
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//split the pair of keys
			String[] parts = key.toString().split(",",2);
			String key1 = parts[0];
			String key2 = parts[1];
			
			
			//from the Hashmap we created earlier, get the associated value of the two keys
			String file1 = map.get(key2.toString());
			String file = map.get(key1.toString());
			
			
			//calculate the similarity for the pair
			double sim = Jaccardsim(file,file1);
			if (sim>0.8){
			context.write(key,sim); //output the pair of key and the Jaccard Similarity if >0.8
			}
			context.getCounter(COUNTERA.LINES).increment(1);
			}
		}
	}
   

 
