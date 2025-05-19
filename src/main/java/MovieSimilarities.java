import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MovieSimilarities {

    // UserID::MovieID::Rating::Timestamp -> <user, (movie:rating)>
    public static class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // ml-100k/u.data  Format: userID \t movieID \t rating \t timestamp
            // String[] fields = value.toString().split("\t");
            // ml-1m/ratings.dat Format: UserID::MovieID::Rating::Timestamp
            String[] fields = value.toString().split("::");
            if (fields.length < 3) return;
            int userID = Integer.parseInt(fields[0]);
            int movieID = Integer.parseInt(fields[1]);
            String rating = fields[2];
            context.write(new IntWritable(userID), new Text(movieID + ":" + rating)); // <user, (movie:rating)>
        }
    }

    // group by users
    public static class Step1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder movieList = new StringBuilder();

            for (Text val : values) {
                if (movieList.length() > 0) {
                    movieList.append(",");
                }
                movieList.append(val.toString());
            }

            context.write(key, new Text(movieList.toString())); // <user, movie1:rating1,movie2:rating2...>
        }
    }

    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text moviePair = new Text();
        private Text ratingPair = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input format: userID \t movie1:rating1,movie2:rating2,...
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 2) return;

            String[] movieRatings = tokens[1].split(","); // movie1:rating1
            int n = movieRatings.length;

            // iterate movie ratings for the user
            for (int i = 0; i < n; i++) {
                String[] movieRating1 = movieRatings[i].split(":");
                if (movieRating1.length != 2) continue;

                String movie1 = movieRating1[0];
                String rating1 = movieRating1[1];

                for (int j = i + 1; j < n; j++) {
                    String[] movieRating2 = movieRatings[j].split(":");
                    if (movieRating2.length != 2) continue;

                    String movie2 = movieRating2[0];
                    String rating2 = movieRating2[1];

                    // Ensure consistent ordering (for symmetry)
                    String movieKey = (Integer.parseInt(movie1) < Integer.parseInt(movie2)) ?
                            movie1 + "," + movie2 : movie2 + "," + movie1;

                    String ratingVal = (Integer.parseInt(movie1) < Integer.parseInt(movie2)) ?
                            rating1 + "," + rating2 : rating2 + "," + rating1;

                    moviePair.set(movieKey);
                    ratingPair.set(ratingVal);

                    context.write(moviePair, ratingPair);
                }
            }
        }
    }

    // group by movie pairs <(movie1,movie2), (rating1,rating2)>, calculate similarity score
    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum_xx = 0;
            double sum_yy = 0;
            double sum_xy = 0;
            int numPairs = 0;

            for (Text val : values) {
                String[] ratings = val.toString().split(",");
                if (ratings.length != 2) continue;

                try {
                    double rating1 = Double.parseDouble(ratings[0]);
                    double rating2 = Double.parseDouble(ratings[1]);

                    sum_xx += rating1 * rating1;
                    sum_yy += rating2 * rating2;
                    sum_xy += rating1 * rating2;
                    numPairs++;
                } catch (NumberFormatException e) {
                    // Skip malformed rating pairs
                }
            }

            double score = 0.0;
            double denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy);

            if (denominator != 0) {
                score = sum_xy / denominator;
            }

            // filter by  co-rating count and score threshold
            if (numPairs > 10 && score > 0.95) {
                String result = String.format("%.4f\t%d", score, numPairs);
                context.write(key, new Text(result)); // <(movie1,movie2), score \t numPairs>
            }
        }
    }

    // load movie name, then map name with movie id <composite(movie1Name,score), (movie2Name \t coRatings)>
    public static class Step3Mapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

        private Map<Integer, String> movieNames = new HashMap<>();
        private static final Logger log = Logger.getLogger(Step3Mapper.class);

        @Override
        protected void setup(Context context) throws IOException {
            // Load movie names file from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    log.info("Distributed cache file: " + cacheFile.toString());
                    // ml-100k/u.item
                    // if (cacheFile.getPath().endsWith("u.item")) {
                    // ml-1m/movies.dat
                    if (cacheFile.getPath().endsWith("movies.dat")) {
                        File file = new File(new File(cacheFile.getPath()).getName());
                        log.info("Trying to read file: " + file.getAbsolutePath());
                        log.info("File exists? " + file.exists());
                        BufferedReader reader = new BufferedReader(new FileReader(file));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            //  ml-100k/u.item format: movieID | movieName
                            // String[] fields = line.split("\\|");
                            // ml-1m/movies.dat format: MovieID::Title::Genres
                            String[] fields = line.split("::");
                            if (fields.length > 1) {
                                movieNames.put(Integer.parseInt(fields[0]), fields[1]);
                            }
                        }
                        reader.close();
                    }
                }
            } else {
                log.warn("No distributed cache files found");
            }

            log.info("Loaded movieNames: " + movieNames.size()); // test size should be 1682
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input line format: "101,103\t0.9765\t23", \t score \t numPairs
            String[] parts = value.toString().split("\t");
            if (parts.length != 3) return;

            String[] moviePair = parts[0].split(",");
            if (moviePair.length != 2) return;

            int movie1 = Integer.parseInt(moviePair[0]);
            int movie2 = Integer.parseInt(moviePair[1]);
            String movie1Name = movieNames.getOrDefault(movie1, "Unknown");
            String movie2Name = movieNames.getOrDefault(movie2, "Unknown");

            int coRatings = Integer.parseInt(parts[2]);
            double score = Double.parseDouble(parts[1]);

            CompositeKeyWritable compositeKey = new CompositeKeyWritable(movie1Name, score);
            context.write(compositeKey, new Text(movie2Name + "\t" + coRatings));
        }
    }

    // partition by movie1 key
    public static class MoviePartitioner extends Partitioner<CompositeKeyWritable, Text> {
        @Override
        public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
            return Math.abs(key.getMovieA().hashCode() % numPartitions);
        }
    }

    public static class MovieGroupingComparator extends WritableComparator {
        protected MovieGroupingComparator() {
            super(CompositeKeyWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKeyWritable k1 = (CompositeKeyWritable) a;
            CompositeKeyWritable k2 = (CompositeKeyWritable) b;
            return k1.getMovieA().compareTo(k2.getMovieA());
        }
    }

    // <movie1Name, movie2Name \t score \t coRatings>
    public static class Step3Reducer extends Reducer<CompositeKeyWritable, Text, Text, Text> {
        private Text currentMovie = new Text();
        private int topN = 10;

        @Override
        protected void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            currentMovie.set(key.getMovieA());

            int count = 0;
            for (Text val : values) {
                if (count++ >= topN) break;

                // val = movieBName \t coRatings
                String[] parts = val.toString().split("\t");
                if (parts.length != 2) continue;
                String movie2Name = parts[0];
                String coRatings = parts[1];
                double score = key.getScore().get();

                if (Integer.parseInt(coRatings) >= 50 && score >= 0.95) { // filter when it's ml-1m
                    // Output format: movie1Name \t movie2Name \t score \t coRatings
                    String output = String.format("%s\t%.4f\t%s", movie2Name, score, coRatings);
                    context.write(currentMovie, new Text(output)); // <movie1Name, movie2Name \t score \t coRatings>
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Step 1: Group user ratings");
        job1.setJarByClass(MovieSimilarities.class);
        job1.setMapperClass(Step1Mapper.class);
        job1.setReducerClass(Step1Reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(10); // ml-1m: 12 machines in total
        FileInputFormat.addInputPath(job1, new Path(args[0])); // ml-100k/u.data
        FileOutputFormat.setOutputPath(job1, new Path("step1_output"));
        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "Step 2: Create movie pairs and compute similarity");
        job2.setJarByClass(MovieSimilarities.class);
        job2.setMapperClass(Step2Mapper.class);
        job2.setReducerClass(Step2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(10); // ml-1m: 12 machines in total
        FileInputFormat.addInputPath(job2, new Path("step1_output"));
        FileOutputFormat.setOutputPath(job2, new Path("step2_output"));
        if (!job2.waitForCompletion(true)) System.exit(1);

        Job job3 = Job.getInstance(conf, "Step 3: Secondary sort and output top-N similar movies");
        job3.setJarByClass(MovieSimilarities.class);
        job3.setMapperClass(Step3Mapper.class);
        job3.setReducerClass(Step3Reducer.class);
        job3.setMapOutputKeyClass(CompositeKeyWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setPartitionerClass(MoviePartitioner.class);
        job3.setGroupingComparatorClass(MovieGroupingComparator.class);
        // job3.addCacheFile(new URI("u.item")); // for local debugging, -files: ml-100k
        // job3.addCacheFile(new URI("movies.dat")); // for local debugging, -files: ml-1m
        // job3.addCacheFile(new URI("s3://moviename/test/u.item")); // for EMR deploy: ml-100k
        job3.addCacheFile(new URI("s3://moviename/ml-1m/movies.dat")); // for EMR deploy: ml-1m
        job3.setNumReduceTasks(10); // ml-1m: 12 machines in total

        FileInputFormat.addInputPath(job3, new Path("step2_output"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
