import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

// Important Assumption: The input files are named in the format "Title_Year.txt"
// Because the instructions dont say where to get the year from, I made this assumption
// Also Driver wasn't stated in the instructions, so I wasn't sure if it was needed

// Time Complextiy: O(N log N) because of sorting in shuffle and sort phase
// Data Structures: Composite Key (Word, Year) so all matching words go to the same place but can still be sorted in descending order by year 
// & HashSet for quick lookup of processed book titles
// Extensibility: Can add other data like Genre or Author to value, then the reducer can filter based on that

public class LibrarySearchEngine {

    // Custom WritableComparable (Composite Key)
    public static class WordYearKey implements WritableComparable<WordYearKey> {
        
        private String word;
        private int year;

        public WordYearKey() { 
            this.word = "";
            this.year = 0;
        }

        public WordYearKey(String word, int year) {
            this.word = word;
            this.year = year;
        }
        // Like slides mention, need to implement write and readFields for Writable
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.word);
            out.writeInt(this.year);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.word = in.readUTF();
            this.year = in.readInt();
        }
        // Overrrides for equals, compareTo, and hashCode taken from slides
        @Override
        public boolean equals(Object o) {
            if (o instanceof WordYearKey) {
                WordYearKey other = (WordYearKey) o;
                return this.word.equals(other.word) && this.year == other.year;
            }
            return false;
        }

        @Override
        public int compareTo(WordYearKey other) {
            int wordCmp = this.word.compareTo(other.word);
            if (wordCmp != 0) {
                return wordCmp;
            }
            
            // From slides, compare then multiply by -1 for descending order
            int yearCmp;
            if (this.year < other.year) {
                yearCmp = -1;
            } else if (this.year == other.year) {
                yearCmp = 0;
            } else {
                yearCmp = 1;
            }
            return -1 * yearCmp; 
        }

        @Override
        public int hashCode() {
            return this.word.hashCode() * 163 + this.year; 
        }

        public void set(String word, int year) {
            this.word = word;
            this.year = year;
        }
        
        public String getWord() { return word; }
        public int getYear() { return year; }
        // From slides, so its easier to read output
        @Override
        public String toString() { return word + "\t" + year; }
    }

    // Mapper
    public static class SearchMapper extends Mapper<Object, Text, WordYearKey, Text> {
        
        private WordYearKey compositeKey = new WordYearKey();
        private Text bookTitle = new Text();
        private int bookYear;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            // Based on the assumption that file name is in the format: Title_Year.txt
            try {
                String cleanName = fileName.replace(".txt", "");
                String[] parts = cleanName.split("_");
                bookTitle.set(parts[0]);
                bookYear = Integer.parseInt(parts[1]);
            } catch (Exception e) {
                bookTitle.set(fileName);
                bookYear = 0;
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString().toLowerCase();
            
            // Using theStringTokenizer constructor from the TF-IDF doc
            StringTokenizer itr = new StringTokenizer(text, " \t\n\r\f,.:;?![]'\"(){}");

            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();
                if (term.length() > 0) {
                    compositeKey.set(term, bookYear);
                    context.write(compositeKey, bookTitle);
                }
            }
        }
    }

    // Combiner
    public static class SearchCombiner extends Reducer<WordYearKey, Text, WordYearKey, Text> {
        @Override
        public void reduce(WordYearKey key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            String previousTitle = "";
            for (Text val : values) {
                // Prevents duplicate entries for same book title
                if (!val.toString().equals(previousTitle)) {
                    context.write(key, val);
                    previousTitle = val.toString();
                }
            }
        }
    }

    // Partitioner
    public static class SearchPartitioner extends Partitioner<WordYearKey, Text> {
        @Override
        public int getPartition(WordYearKey key, Text value, int numReduceTasks) {
            // Partition based only on the word so that all same words go to same reducer
            return (key.getWord().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    // Custom Comparator
    public static class SearchGroupingComparator extends WritableComparator {
        
        public SearchGroupingComparator() {
            super(WordYearKey.class, true);
        }
        // Because compareTo was overridden, don't need to do -1 * here
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            WordYearKey k1 = (WordYearKey) a;
            WordYearKey k2 = (WordYearKey) b;
            return k1.getWord().compareTo(k2.getWord());
        }
    }

    // Reducer
    public static class SearchReducer extends Reducer<WordYearKey, Text, Text, Text> {
        
        private Text result = new Text();
        private Text outputKey = new Text();

        @Override
        public void reduce(WordYearKey key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            StringBuilder sb = new StringBuilder();
            Set<String> processedBooks = new HashSet<>();
            boolean first = true;

            for (Text val : values) {
                String title = val.toString();
                
                if (processedBooks.contains(title)) {
                    continue;
                }
                
                if (!first) {
                    sb.append(", ");
                }
                // Since values arrive at the reducer sorted by year and grouped by word, just need to append them to stringbuilder
                sb.append(title).append(" (").append(key.getYear()).append(")");
                processedBooks.add(title);
                first = false;
            }

            outputKey.set(key.getWord());
            result.set(sb.toString());
            // (Word, "Title1 (Year1), Title2 (Year2), ...")
            context.write(outputKey, result);
        }
    }
}