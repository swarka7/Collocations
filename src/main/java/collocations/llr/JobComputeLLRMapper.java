package collocations.llr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Mapper-only job that attaches N and computes LLR for each bigram record.
 */
public class JobComputeLLRMapper extends Mapper<Object, Text, Text, Text> {
    private final Map<String, Long> nByLangDecade = new HashMap<>();
    private final Text outKey = new Text();
    private final Text outVal = new Text();
    private boolean printedMissingN = false;
    private int missingNCount = 0;
    private final Counter nLoadedCounter = null;

    @Override
    protected void setup(Context context) throws IOException {
        boolean loadedFromCache = loadFromCache(context);
        if (!loadedFromCache) {
            loadFromCountsDir(context);
        }
        System.err.println("LLR_SETUP loaded N entries: " + nByLangDecade.size());
        int printed = 0;
        for (Map.Entry<String, Long> e : nByLangDecade.entrySet()) {
            if (printed >= 5) break;
            System.err.println("LLR_SETUP sample N: " + e.getKey() + " -> " + e.getValue());
            printed++;
        }
        context.getCounter("LLR", "N_LOADED").increment(nByLangDecade.size());
    }

    private boolean loadFromCache(Context context) {
        try {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return false;
            }
            boolean any = false;
            for (URI uri : cacheFiles) {
                any |= loadNFromStream(uri.toURL().openStream());
            }
            return any;
        } catch (Exception e) {
            System.err.println("LLR_SETUP cache load failed: " + e.getMessage());
            return false;
        }
    }

    private void loadFromCountsDir(Context context) {
        String dir = context.getConfiguration().get("colloc.counts.dir");
        if (dir == null) {
            return;
        }
        try {
            Path path = new Path(dir);
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FileStatus[] statuses = fs.globStatus(path);
            if (statuses == null || statuses.length == 0) {
                if (fs.isDirectory(path)) {
                    statuses = fs.listStatus(path);
                }
            }
            if (statuses == null) {
                System.err.println("LLR_SETUP counts-dir: no statuses for " + dir);
                return;
            }
            for (FileStatus st : statuses) {
                if (st.isDirectory()) {
                    // drill down one level
                    for (FileStatus inner : fs.listStatus(st.getPath())) {
                        if (inner.isFile()) {
                            loadNFromFile(fs, inner.getPath());
                        }
                    }
                } else if (st.isFile()) {
                    loadNFromFile(fs, st.getPath());
                }
            }
        } catch (Exception e) {
            System.err.println("LLR_SETUP counts-dir load failed: " + e.getMessage());
        }
    }

    private void loadNFromFile(FileSystem fs, Path p) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\t");
                if (parts.length < 4) continue;
                if (!"N".equals(parts[2])) continue;
                long n;
                try {
                    n = Long.parseLong(parts[parts.length - 1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                nByLangDecade.put(parts[0] + "\t" + parts[1], n);
            }
        } catch (IOException ignore) {
        }
    }

    private boolean loadNFromStream(java.io.InputStream stream) {
        boolean any = false;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\t");
                if (parts.length < 4) continue;
                if (!"N".equals(parts[2])) continue;
                long n;
                try {
                    n = Long.parseLong(parts[parts.length - 1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                nByLangDecade.put(parts[0] + "\t" + parts[1], n);
                any = true;
            }
        } catch (IOException ignore) {
        }
        return any;
    }

    @Override
    protected void map(Object offset, Text line, Context context) throws IOException, InterruptedException {
        // line: lang \t decade \t w1 \t w2 \t c12 \t c1 \t c2
        String[] parts = line.toString().split("\\t");
        if (parts.length < 7) {
            return;
        }
        String lang = parts[0];
        String decade = parts[1];
        String w1 = parts[2];
        String w2 = parts[3];
        long c12;
        long c1;
        long c2;
        try {
            c12 = Long.parseLong(parts[4]);
            c1 = Long.parseLong(parts[5]);
            c2 = Long.parseLong(parts[6]);
        } catch (NumberFormatException e) {
            return;
        }

        Long nVal = nByLangDecade.get(lang + "\t" + decade);
        if (nVal == null || nVal <= 0) {
            if (!printedMissingN && missingNCount < 10) {
                System.err.println("LLR_MISSING_N lang=" + lang + " decade=" + decade);
                missingNCount++;
                if (missingNCount >= 10) {
                    printedMissingN = true;
                }
            }
            context.getCounter("LLR", "MISSING_N").increment(1);
            return;
        }
        context.getCounter("LLR", "HAVE_N").increment(1);
        long n = nVal;
        double llr = computeLLR(c12, c1, c2, n);
        outKey.set(lang + "\t" + decade + "\t" + w1 + "\t" + w2);
        outVal.set(Double.toString(llr));
        context.write(outKey, outVal);
    }

    private double computeLLR(long n11, long n1dot, long nDot1, long n) {
        long n12 = n1dot - n11;
        long n21 = nDot1 - n11;
        long n22 = n - n11 - n12 - n21;
        double ll = 0.0;
        ll += scoreCell(n11, n1dot, nDot1, n);
        ll += scoreCell(n12, n1dot, n - nDot1, n);
        ll += scoreCell(n21, n - n1dot, nDot1, n);
        ll += scoreCell(n22, n - n1dot, n - nDot1, n);
        return 2.0 * ll;
    }

    private double scoreCell(long observed, long rowTotal, long colTotal, long n) {
        if (observed <= 0) {
            return 0.0;
        }
        double expected = (double) rowTotal * (double) colTotal / (double) n;
        if (expected <= 0) {
            return 0.0;
        }
        return observed * Math.log(observed / expected);
    }
}
