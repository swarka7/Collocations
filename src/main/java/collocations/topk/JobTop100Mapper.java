package collocations.topk;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Parses LLR output and keys by (lang, decade).
 */
public class JobTop100Mapper extends Mapper<Text, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // key: lang \t decade \t w1 \t w2
        String[] keyParts = key.toString().split("\\t");
        if (keyParts.length != 4) {
            return;
        }
        String lang = keyParts[0];
        String decade = keyParts[1];
        String w1 = keyParts[2];
        String w2 = keyParts[3];
        outKey.set(lang + "\t" + decade);
        outVal.set(w1 + "\t" + w2 + "\t" + value.toString());
        context.write(outKey, outVal);
    }
}
