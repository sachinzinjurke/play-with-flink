# play-with-flink
Sample project on Apache Flink batch features like join/group examples

Project will use sample data downloaded from below link
https://grouplens.org/datasets/movielens/latest/

Link contains few data files out of which we are using movie and rating dataset files.
It has two exmples :
1. FlinkDramaMovieParser.java
2. FlinkJoinByGroupByExample.java

FlinkDramaMovieParser:
Example will parse the movie data set and filter out only drama movies.

FlinkJoinByGroupByExample:
Example will show how we can use join and group by features of apache flink on dataset.
It will join two datasets movies and rattings. Group the join result on average rating against a particular genre. Finally will the the result
