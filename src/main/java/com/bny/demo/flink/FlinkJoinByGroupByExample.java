package com.bny.demo.flink;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/*
 * Example will give us the movies with favorite genre(rating on genre).
 * Steps to achieve this:
 * 
 * 1. Join movies and rating data set on movie id
 * 2. Group by resulted collection from Step-1 by Genre
 * 3. Collect the Step -2 result as <Genre,Average Rating>
 * 4. Print the Step-3 result
 * 5. Sort the result of Step-4 so that will get most liked genre on top
 */


public class FlinkJoinByGroupByExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple3<Long,String,String>> movie=env.readCsvFile("D://apache-flink//play-with-flink//src//main//resources//movies.csv")
		.ignoreFirstLine()
		.parseQuotedStrings('"')
		.ignoreInvalidLines()
		.types(Long.class,String.class,String.class);
		
		
		DataSet<Tuple2<Long,Double>> rating=env.readCsvFile("D://apache-flink//play-with-flink//src//main//resources//ratings.csv")
				.ignoreFirstLine()
				.includeFields(false,true,true,false)
				.types(Long.class,Double.class);
		
	
		
		List<Tuple2<String,Double>>distribution=   movie.join(rating).where(0).equalTo(0).with(new JoinFunction<Tuple3<Long,String,String>,Tuple2<Long,Double>,Tuple3<String,String,Double>>() {
			@Override
			public Tuple3<String,String,Double> join(Tuple3<Long,String,String>movie,Tuple2<Long,Double>rating) throws Exception {
				
				String movieName=movie.f1;
				String genre=movie.f2.split("\\|")[0];
				Double rat=rating.f1;
				return new Tuple3<>(movieName,genre,rat);
			}
		})
		.groupBy(1)
		.reduceGroup(new GroupReduceFunction<Tuple3<String,String,Double>, Tuple2<String,Double>>() {

			@Override
			public void reduce(Iterable<Tuple3<String, String, Double>> iterable,
					Collector<Tuple2<String, Double>> collector) throws Exception {
				
				String genre=null;
				int count=0;
				double totalScore=0;
				
				for (Tuple3<String, String, Double> groupedMovieByGenre : iterable) {
					genre=groupedMovieByGenre.f1;
					count++;
					totalScore+=groupedMovieByGenre.f2;
				}
				
				collector.collect(new Tuple2<>(genre,totalScore/count));
			}
		
			
		}).collect()	;
		
		for (Tuple2<String, Double> tuple2 : distribution) {
			System.out.println(tuple2);
		}
		
		System.out.println("**********Sorting distribution list*************");
		
		String result=distribution.stream()
		.sorted((r1,r2)->Double.compare(r1.f1, r2.f1))
		.map(Object::toString)
		.collect(Collectors.joining("\n"));
		
		System.out.println(result);
		
	}
}
