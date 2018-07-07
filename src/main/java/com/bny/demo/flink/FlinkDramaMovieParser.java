package com.bny.demo.flink;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import com.bny.demo.modal.Movie;

public class FlinkDramaMovieParser {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple3<Long,String,String>> dataset=env.readCsvFile("D://apache-flink//play-with-flink//src//main//resources//movies.csv")
		.ignoreFirstLine()
		.parseQuotedStrings('"')
		.ignoreInvalidLines()
		.types(Long.class,String.class,String.class);
		
		DataSet<Movie> movieDataSet=dataset.map(new MapFunction<Tuple3<Long,String,String>, Movie>() {
			private static final long serialVersionUID = 161110595623060603L;
			@Override
			public Movie map(Tuple3<Long, String, String> tuple)throws Exception {
				String movieName=tuple.f1;
				String [] genres=tuple.f2.split("\\|");
				Movie movie=new Movie(movieName,new HashSet<>(Arrays.asList(genres)));
				return movie;
			}
		});
		DataSet<Movie> dramaDataSet=movieDataSet.filter(new FilterFunction<Movie>() {
			private static final long serialVersionUID = -4226295216317939816L;
			@Override
			public boolean filter(Movie movie) throws Exception {
				return movie.getGenres().contains("Drama");
			}
		});
		
		dramaDataSet.print();
		
	}
}
