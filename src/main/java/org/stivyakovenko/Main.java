package org.stivyakovenko;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static class Rating implements Serializable {
        private int userId;
        private int movieId;
        private float rating;
        private long timestamp;

        public Rating() {}

        public Rating(int userId, int movieId, float rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public int getUserId() {
            return userId;
        }

        public int getMovieId() {
            return movieId;
        }

        public float getRating() {
            return rating;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public static Rating parseRating(String str) {
            String[] fields = str.split(",");
            if (fields.length != 4) {
                throw new IllegalArgumentException("Each line must contain 4 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            float rating = Float.parseFloat(fields[2]);
            long timestamp = Long.parseLong(fields[3]);
            return new Rating(userId, movieId, rating, timestamp);
        }
    }

    static String parse(String str) {
        Pattern pat = Pattern.compile("\\[[0-9.]*,[0-9.]*]");
        Matcher matcher = pat.matcher(str);
        int count = 0;
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            count++;
            String substring = str.substring(matcher.start(), matcher.end());
            String itstr = substring.split(",")[0].substring(1);
            sb.append(itstr + " ");
        }
        return sb.toString().trim();
    }

    static TreeMap<Long, String> res = new TreeMap<>();

    public static void add(long k, String v) {
        res.put(k, v);
    }

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession spark = SparkSession
                .builder()
                .appName("SomeAppName")
                .config("spark.master", "local")
                .getOrCreate();
        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile(args[0]).javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
        ALS als = new ALS()
                .setMaxIter(1)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(ratings);
        model.setColdStartStrategy("drop");
        Dataset<Row> rowDataset = model.recommendForAllUsers(50);
        rowDataset.foreach((ForeachFunction<Row>) row -> {
            String str = row.toString();
            long l = Long.parseLong(str.substring(1).split(",")[0]);
            add(l, parse(str));
        });
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
        for (long l = 0; l < res.lastKey(); l++) {
            if (!res.containsKey(l)) {
                bw.write("\n");
                continue;
            }
            String str = res.get(l);
            bw.write(str);
        }
        bw.close();
    }
}

