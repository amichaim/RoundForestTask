import org.amichai.roundforest.amazonreviwes.ReviewsAnalyzer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ReviewAnalyzerTest {

    @Test
    public void testReviewAnalyzet() {
        ReviewsAnalyzer reviewsAnalyzer = new ReviewsAnalyzer("C:\\roundforest\\data\\Reviews.csv", 1000, 1000, 1000);
        reviewsAnalyzer.init();

        List<String> mostActiveUsers = reviewsAnalyzer.getMostActiveUsers();
        List<String> mostCommentedItems = reviewsAnalyzer.getMostCommentedItems();
        List<String> mostUsedWords = reviewsAnalyzer.getMostUsedWords();

        Assert.assertEquals(1000, mostActiveUsers.size());
        Assert.assertEquals(1000, mostCommentedItems.size());
        Assert.assertEquals(1000, mostUsedWords.size());

        Assert.assertEquals("\"C. F. Hill \"\"CFH\"\"\"", mostActiveUsers.get(0));
        Assert.assertEquals("B007JFMH8M", mostCommentedItems.get(0));
        Assert.assertEquals("the", mostUsedWords.get(0));

    }

}
