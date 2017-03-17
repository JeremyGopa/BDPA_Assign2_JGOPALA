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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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



public class SimilarityB extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SimilarityB(), args);
      
      System.exit(res);
   }
   
   public static enum COUNTERB {
	   LINES,
	 };
	 
	 
	 //create a HashMap with line number as key and words as value (put input file into HashMap)
	public static HashMap<String, String> map = new HashMap<String, String>();{
		BufferedReader reader;
		try {
			reader = new BufferedReader( new FileReader("/home/cloudera/workspace/output_prepro.txt"));
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] parts1 = line.split(",");
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
	Job job = new Job(getConf(), "SimilarityB");
      job.setJarByClass(SimilarityB.class);
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
	      File file = new File("/home/cloudera/workspace/NBComparisonB.txt");
	      file.createNewFile();
	      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
	      long lines = job.getCounters().findCounter(COUNTERB.LINES)
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
    	  
    	  // split input into key, value
    	  String[] parts = value.toString().split(",");
		  String key1 = parts[0];
		  String value1 = parts[1];		
		  
		  //split words and put them in a list
    	  List<String> list = Arrays.asList(value1.toString().split(" "));
    	  
    	  //calculate number of words to keep
    	  int keep = (int) Math.round((list.size() - (list.size()*0.8) + 1));
    	  
    	  for (int i=list.size();i>list.size()-keep;i--){ //take words from last to first (words with higher frequency are at the end of the line)
    		  context.write(new Text(list.get(i-1)),new Text(key1.toString())); //output the word as key and the lines they appear in as value
    	  }
      }
   }
   
   public static class Reduce extends Reducer<Text, Text, Text, Double> {
	   
	   private HashMap<Text, Double> uniquepair = new HashMap<Text, Double>(); //HashMap used later to avoid duplicates
	   
	   
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
			
			//get the line number of words and sort them into ascending order to get the same output format as Similarity A ie:smaller key first
			ArrayList<Integer> list = new ArrayList<Integer>();
			for (Text value : values) {
				list.add(Integer.parseInt(value.toString()));
			}
			Collections.sort(list);
			
			
			// put the line number as a pair 
			List<Pair<String, String>> words = new ArrayList<Pair<String, String>>();
			for (int val=0;val<list.size()-1;val++){
				for (int val1=val+1;val1<=list.size()-1;val1++){
					String key1 = list.get(val).toString();
					String key2 = list.get(val1).toString();
					words.add(new Pair<String, String>(key1,key2));
				}
			}
			
			
			for (int i=0;i<=words.size()-1;i++){
				if (uniquepair.containsKey(new Text(words.get(i).getFirst()+","+words.get(i).getSecond()))){	
				}
				else{
				String file1 = map.get(words.get(i).getFirst());
				String file2 = map.get(words.get(i).getSecond());
				double sim = Jaccardsim(file1,file2);
				if (sim>0.8){
					context.write(new Text(words.get(i).getFirst()+","+words.get(i).getSecond()),sim);
					uniquepair.put(new Text(words.get(i).getFirst()+","+words.get(i).getSecond()),sim);
					}
				context.getCounter(COUNTERB.LINES).increment(1);
				}
			}	

		}
	}
}

