package coverage_stat;

import org.apache.spark.api.java.JavaRDD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import scala.Tuple2;

public class coverage_stat {	
	public static Options options=new Options();
	static {
		options.addOption("h","help",false,"list short help");
		options.addOption("r","read",true,"the bam file path of alignmented reads");
		options.addOption("b", "bed", true, "the bed file path");
		options.addOption("o","output",true,"the output file path");
		options.addOption("s","split",false,"the input splitSize");
	}
	static void printHelp(Options options){
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("Count the rate of ATCG base in every position of the chromosome.", options);
	}
	
	public static JavaPairRDD<Tuple2<String,Integer>,Integer> basecount(JavaRDD<SAMRecord> reads, Broadcast<HashMap<String, Set<Integer>>> bedbroad){
		JavaPairRDD<Tuple2<String, Integer>, Iterable<String>> pairrdd=reads.flatMapToPair(
				new PairFlatMapFunction<SAMRecord, Tuple2<String, Integer>, String>(){
					
					private static final long serialVersionUID = 1L;
					
					Tuple2<String,Integer> key;
					public Iterator<Tuple2<Tuple2<String,Integer>,String>> call(SAMRecord samrecord){
						List<Tuple2<Tuple2<String, Integer>, String>> list=new ArrayList<Tuple2<Tuple2<String,Integer>,String>>();
						String chr = samrecord.getReferenceName();
						String name=samrecord.getReadName();
						byte[] readbase=samrecord.getReadBases();
						int start=samrecord.getAlignmentStart();
						int end=samrecord.getAlignmentEnd();
						for (int j=start; j<end; j++){
							char base=(char)readbase[samrecord.getReadPositionAtReferencePosition(j)];
							key=new Tuple2<String,Integer>(chr,j);
							list.add(new Tuple2<Tuple2<String,Integer>,String>(key,Character.toString(base)));
						}
						return list.iterator();
					}
			}).filter(v -> bedbroad.getValue().get(v._1._1).contains(v._1._2)).groupByKey();
		JavaPairRDD<Tuple2<String,Integer>, Integer> countrdd=pairrdd.mapValues(new Function<Iterable<String>,Integer>(){
			
			private static final long serialVersionUID = 1L;

			public Integer call(Iterable<String> list){
				int count=0;
				for(String x:list){
					count+=1;
				}
				return count;	
			}
		});
		return countrdd;
	}
	
	public static HashMap<String, Set<Integer>> bedparse(String bedfile) throws IOException {
		File bed = new File(bedfile);
		BufferedReader bedreader = new BufferedReader(new FileReader(bed));
		HashMap<String, Set<Integer>> bedHashMap = new HashMap<String, Set<Integer>>();
		String line;
		while ((line = bedreader.readLine())!=null) {
			String[] num = line.split("\t");
			String key = num[0];
			int pos1=Integer.parseInt(num[1]);
			int pos2=Integer.parseInt(num[2]);
			int start=Math.min(pos1, pos2);
			int end=Math.max(pos1, pos2);
			Set<Integer> positions = bedHashMap.containsKey(key) ? bedHashMap.get(key) : new HashSet<Integer>();
			for (int i=start ; i<=end; i++) {
				positions.add(i);
			}
			bedHashMap.put(key, positions);
		}
		bedreader.close();
		return bedHashMap;
	}
	
	/**
	 * the count of depth on every position 	
	 * @throws IOException 
	*/
	
	public static void main(String[] args) throws ParseException, IOException{
		CommandLineParser parser = new PosixParser();
		CommandLine cl = parser.parse(options, args);
		if (cl.hasOption("h") || !cl.hasOption("r") || !cl.hasOption("o")){
			printHelp(options);
			return;
		}
		String inputdir=cl.getOptionValue("r");
		String outputfile=cl.getOptionValue("o");
		String bedfile=cl.hasOption("b") ? cl.getOptionValue("b"): "" ;
		int splitsize=cl.hasOption("s")? Integer.parseInt(cl.getOptionValue("s")) : 0;
		
		JavaSparkContext ctx=new JavaSparkContext();
		Configuration conf = ctx.hadoopConfiguration();
		if (splitsize!=0) {
			conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(splitsize));
		}
		JavaRDD<SAMRecord> read1 = ctx.newAPIHadoopFile(inputdir, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, conf).
				map(v -> v._2.get()).filter(v -> v!=null);
		HashMap<String, Set<Integer>> bedHashMap = null;
		if (!bedfile.equals("")) {
			bedHashMap = bedparse(bedfile);
		}
		Broadcast<HashMap<String, Set<Integer>>> bedbroad = ctx.broadcast(bedHashMap);
		JavaPairRDD<Tuple2<String,Integer>,Integer> countrdd=basecount(read1, bedbroad).sortByKey(new Comparator<Tuple2<String,Integer>>(){

			@Override
			public int compare(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) {
				// TODO Auto-generated method stub
				if (arg0._1.equals(arg1._1)) {
					return arg0._2.compareTo(arg1._2);
				}else {
					return arg0._1.compareTo(arg1._1);
				}
			}
			
		});
		JavaRDD<String> stringrdd = countrdd.map(v -> v._1._1 + "\t" + v._1._2.toString() + "\t" + v._2.toString());
		stringrdd.saveAsTextFile(outputfile);
		ctx.close();
	}

}
