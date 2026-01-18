package collocations.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Keeps top 100 records per (lang, decade) by llr using a bounded min-heap.
 */
public class JobTop100Reducer extends Reducer<Text, Text, Text, Text> {
    private static final int K = 100;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        PriorityQueue<Scored> heap = new PriorityQueue<>(K, Comparator.comparingDouble(s -> s.llr));
        for (Text val : values) {
            String[] parts = val.toString().split("\\t");
            if (parts.length < 3) {
                continue;
            }
            double llr;
            try {
                llr = Double.parseDouble(parts[2]);
            } catch (NumberFormatException e) {
                continue;
            }
            Scored s = new Scored(parts[0], parts[1], llr);
            if (heap.size() < K) {
                heap.add(s);
            } else if (heap.peek().llr < llr) {
                heap.poll();
                heap.add(s);
            }
        }
        List<Scored> results = new ArrayList<>(heap);
        results.sort((a, b) -> Double.compare(b.llr, a.llr));
        for (Scored s : results) {
            context.write(key, new Text(s.w1 + "\t" + s.w2 + "\t" + s.llr));
        }
    }

    private static class Scored {
        final String w1;
        final String w2;
        final double llr;

        Scored(String w1, String w2, double llr) {
            this.w1 = w1;
            this.w2 = w2;
            this.llr = llr;
        }
    }
}
