import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joshua on 6/29/17.
 */

class ConvertWordstoAds {
    public class LuceneConstants {
        static final String CONTENTS = "contents";
        public static final String FILE_NAME = "filename";
        public static final String FILE_PATH = "filepath";
        public static final int MAX_SEARCH = 10;
    }

    static List<String> cleanAndTokenizeData(String text) throws IOException {
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
        TokenStream tokenStream = analyzer.tokenStream(
                LuceneConstants.CONTENTS, new StringReader(text));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
        List<String> keywords = new ArrayList<>();
        while(tokenStream.incrementToken()) {
            keywords.add(String.valueOf(term));
        }
        return keywords;
    }
}