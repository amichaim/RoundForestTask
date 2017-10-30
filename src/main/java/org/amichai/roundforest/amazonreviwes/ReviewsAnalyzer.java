package org.amichai.roundforest.amazonreviwes;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.util.*;

import static org.apache.spark.sql.functions.desc;

public class ReviewsAnalyzer {

    private int numOfActiveUsers;
    private int numOfCommentedItems;
    private int numOfWords;
    private String dataFilePath;
    private int numOfCores;

    Dataset<Row> reviews;

    public ReviewsAnalyzer(String dataFilePath, int numOfActiveUsers, int numOfCommentedItems, int numOfWords, int numOfCores) {
        this.dataFilePath = dataFilePath;
        this.numOfActiveUsers = numOfActiveUsers;
        this.numOfCommentedItems = numOfCommentedItems;
        this.numOfWords = numOfWords;
        this.numOfCores = numOfCores;
    }

    public void init() {
        SparkConf sparkConf = new SparkConf();
        SparkSession.Builder builder = SparkSession.builder().appName("RoundForest Amazon Reviews Analyzer")
                .config(sparkConf);
        builder.master("local[*]");//use all available cores
        SparkSession spark = builder.getOrCreate();

        //read raw data
        reviews = spark.read().csv(dataFilePath);

        //repartition to 3 times number of cores - this the recommendation
        reviews = reviews.repartition(numOfCores * 3);

        //drop duplicates when key is productId and userId
        //currently commented out because it cause performance issue
        //todo check how to dropDuplicates with a good performance
        //reviews = reviews.dropDuplicates("_c1", "_c2");

        //cache because we will do few calculations on that dataset
        reviews.cache();
    }

    public List<String> getMostActiveUsers() {
        List<String> result = new ArrayList<String>();

        Dataset<Row> activeUsers = reviews.groupBy("_c3").count().orderBy(desc("count"));
        Iterator<Row> activeUsersIter = activeUsers.toLocalIterator();
        int i = 0;
        while (activeUsersIter.hasNext() && i < numOfActiveUsers) {
            Row row = activeUsersIter.next();
            //System.out.println("Profile Name: " + row.get(0) + ", Reviews: " + row.get(1));
            result.add((String)row.get(0));
            i++;
        }
        return result;
    }

    public List<String> getMostCommentedItems() {
        List<String> result = new ArrayList<String>();

        Dataset<Row> mostCommentedItems = reviews.groupBy("_c1").count().orderBy(desc("count"));
        Iterator<Row> mostCommentedItemsIter = mostCommentedItems.toLocalIterator();
        int i = 0;
        while (mostCommentedItemsIter.hasNext() && i < numOfCommentedItems) {
            Row row = mostCommentedItemsIter.next();
            //System.out.println("ProductId: " + row.get(0) + ", Reviews: " + row.get(1));
            result.add((String)row.get(0));
            i++;
        }
        return result;
    }

    public List<String> getMostUsedWords() {
        List<String> result = new ArrayList<String>();

        Map<String, Integer> reviewWordsCount = new HashMap<String, Integer>();
        Iterator<Row> reviewsIter = reviews.toLocalIterator();
        while (reviewsIter.hasNext()) {
            Row row = reviewsIter.next();
            String review = (String)row.get(9);
            StringTokenizer st = new StringTokenizer(review, " ");
            while (st.hasMoreTokens()) {
                String word = st.nextToken();
                if (reviewWordsCount.get(word) == null) {
                    reviewWordsCount.put(word, 1);
                }
                else {
                    reviewWordsCount.put(word, reviewWordsCount.get(word) + 1);
                }
            }
        }

        Map<String, Integer> sortedReviewWordsCount = sortByValue(reviewWordsCount);

        int i = 0;
        for(Map.Entry<String, Integer> entry : sortedReviewWordsCount.entrySet()) {
            if (i == numOfWords) {
                break;
            }
            //System.out.println("Word: " + entry.getKey() + ", count: " + entry.getValue());
            result.add(entry.getKey());
            i++;
        }
        return result;
    }

    public static void main(String[] args) {

        String dataFilePath = args[0];
        int numOfActiveUsers = Integer.parseInt(args[1]);
        int numOfCommentedItems = Integer.parseInt(args[2]);
        int numOfWords = Integer.parseInt(args[3]);
        int numOfCores = Integer.parseInt(args[4]);

        ReviewsAnalyzer reviewsAnalyzer = new ReviewsAnalyzer(dataFilePath, numOfActiveUsers, numOfCommentedItems, numOfWords, numOfCores);
        reviewsAnalyzer.init();

        List<String> mostActiveUsers = reviewsAnalyzer.getMostActiveUsers();
        List<String> mostCommentedItems = reviewsAnalyzer.getMostCommentedItems();
        List<String> mostUsedWords = reviewsAnalyzer.getMostUsedWords();

        Collections.sort(mostActiveUsers);
        Collections.sort(mostCommentedItems);
        Collections.sort(mostUsedWords);

        System.out.println("Most Active Users:");
        System.out.println();
        for (String user : mostActiveUsers) {
            System.out.println(user);
        }

        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("Most Commented Items:");
        System.out.println();
        for (String item : mostCommentedItems) {
            System.out.println(item);
        }

        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("Most Used Words:");
        System.out.println();
        for (String word : mostUsedWords) {
            System.out.println(word);
        }
    }

    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return -1 * (o1.getValue()).compareTo( o2.getValue() );
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
