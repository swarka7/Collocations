package collocations;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A standalone runner that uploads the shaded jar (if needed), spins up an EMR cluster,
 * and adds all MapReduce steps for both languages in sequence.
 */
public class EmrFlowRunner {

    private static final String REGION = Regions.US_EAST_1.getName();
    // English now uses the British corpus paths per latest request.
    private static final String ENG_INPUT = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data";
    private static final String HEB_INPUT = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static final List<String> LANG_ORDER = Arrays.asList("he", "en");

    private static final String JOB1_MAIN = "collocations.counts.CountsJob";
    private static final String JOB2_MAIN = "collocations.join.JoinByW1Job";
    private static final String JOB3_MAIN = "collocations.join.JoinByW2Job";
    private static final String JOB4_MAIN = "collocations.llr.LLRJob";
    private static final String JOB5_MAIN = "collocations.topk.TopKJob";
    private static final String BUILD_FINGERPRINT = "BUILD_FINGERPRINT=2025-12-20T21:38Z";

    public static void main(String[] args) {
        printDebugPrelude(args);
        CliArgs cli = CliArgs.parse(args);
        printDebugEnv();
        if (!cli.forceLocal && isRunningOnEmrNode()) {
            System.err.println("EmrFlowRunner must be executed locally, not as an EMR step.");
            System.exit(2);
        }
        String runId = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
        String jarS3Uri = cli.jarS3Uri;
        if (jarS3Uri == null) {
            jarS3Uri = String.format("s3://%s/hw2/jars/collocations-%s.jar", cli.bucket, runId);
            uploadJarToS3(s3, cli.jarLocalPath, jarS3Uri);
        }
        System.out.println("DEBUG jarS3Uri=" + jarS3Uri);

        Map<String, Map<String, String>> outputPaths = buildOutputPaths(cli.bucket, runId, cli.runLangs);
        List<StepConfig> steps = buildSteps(jarS3Uri, outputPaths, cli);
        JobFlowInstancesConfig instances = buildInstances(cli);
        printStepCommands(jarS3Uri, steps);

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(REGION)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("collocations-" + runId)
                .withReleaseLabel("emr-6.15.0")
                .withApplications(new Application().withName("Hadoop"))
                .withLogUri(cli.logUri)
                .withSteps(steps)
                .withInstances(instances)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withVisibleToAllUsers(true);

        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("Created EMR cluster (jobFlowId): " + result.getJobFlowId());
        System.out.println("Jar S3 URI: " + jarS3Uri);
        printOutputPaths(outputPaths);
    }

    private static boolean isRunningOnEmrNode() {
        String platform = System.getenv("PLATFORM_TYPE");
        String emrStepId = System.getenv("EMR_STEP_ID");
        String jobId = System.getenv("JOB_IDENTIFIER");
        if ("EMR_ON_EC2".equalsIgnoreCase(platform)) {
            return true;
        }
        if (emrStepId != null && !emrStepId.isEmpty()) {
            return true;
        }
        if (jobId != null && !jobId.isEmpty()) {
            return true;
        }
        return false;
    }

    private static void printDebugEnv() {
        System.out.println("DEBUG os.name=" + System.getProperty("os.name"));
        System.out.println("DEBUG user.name=" + System.getProperty("user.name"));
        System.out.println("DEBUG user.dir=" + System.getProperty("user.dir"));
        System.out.println("DEBUG env.PLATFORM_TYPE=" + System.getenv("PLATFORM_TYPE"));
        System.out.println("DEBUG env.AWS_EXECUTION_ENV=" + System.getenv("AWS_EXECUTION_ENV"));
        System.out.println("DEBUG env.HADOOP_HOME=" + System.getenv("HADOOP_HOME"));
        System.out.println("DEBUG env.EMR_STEP_ID=" + System.getenv("EMR_STEP_ID"));
        System.out.println("DEBUG env.JOB_IDENTIFIER=" + System.getenv("JOB_IDENTIFIER"));
        File stepsDir = new File("/mnt/var/lib/hadoop/steps");
        System.out.println("DEBUG stepsDirExists=" + stepsDir.exists());
    }

    private static void printDebugPrelude(String[] args) {
        System.out.println("DEBUG startTime=" + new Date());
        System.out.println("DEBUG args.length=" + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("DEBUG arg[" + i + "]=" + args[i]);
        }
        System.out.println("DEBUG detectedEmr=" + isRunningOnEmrNode());
    }

    private static JobFlowInstancesConfig buildInstances(CliArgs cli) {
        int totalInstances = Math.max(2, cli.instanceCount);
        int coreCount = Math.max(1, totalInstances - 1);
        List<InstanceGroupConfig> groups = new ArrayList<>();
        groups.add(new InstanceGroupConfig()
                .withName("master")
                .withInstanceCount(1)
                .withInstanceRole(InstanceRoleType.MASTER)
                .withInstanceType(cli.instanceType)
                .withMarket(MarketType.ON_DEMAND));
        groups.add(new InstanceGroupConfig()
                .withName("core")
                .withInstanceCount(coreCount)
                .withInstanceRole(InstanceRoleType.CORE)
                .withInstanceType(cli.instanceType)
                .withMarket(MarketType.ON_DEMAND));

        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withInstanceGroups(groups)
                .withKeepJobFlowAliveWhenNoSteps(false);
        if (cli.keyName != null) {
            config.setEc2KeyName(cli.keyName);
        }
        return config;
    }

    private static void uploadJarToS3(AmazonS3 s3, String localJarPath, String jarS3Uri) {
        if (localJarPath == null) {
            throw new IllegalArgumentException("Missing --jar or --jarS3");
        }
        Path jarPath = Paths.get(localJarPath).toAbsolutePath();
        if (!jarPath.toFile().exists()) {
            throw new IllegalArgumentException("Jar file does not exist: " + jarPath);
        }
        S3UriParts parts = parseS3Uri(jarS3Uri);
        System.out.println("Uploading jar to " + jarS3Uri + " ...");
        s3.putObject(new PutObjectRequest(parts.bucket, parts.key, new File(jarPath.toString())));
        System.out.println("Upload complete.");
    }

    private static Map<String, Map<String, String>> buildOutputPaths(String bucket, String runId, List<String> langs) {
        Map<String, Map<String, String>> langOutputs = new LinkedHashMap<>();
        for (String lang : LANG_ORDER) {
            if (!langs.contains(lang)) {
                continue;
            }
            String base = String.format("s3://%s/collocations/%s/%s", bucket, lang, runId);
            Map<String, String> stages = new LinkedHashMap<>();
            stages.put("job1-counts-nc", base + "/job1-counts-nc");
            stages.put("job1-counts-c", base + "/job1-counts-c");
            stages.put("job2-join1", base + "/job2-join1");
            stages.put("job3-join2", base + "/job3-join2");
            stages.put("job4-llr", base + "/job4-llr");
            stages.put("job5-top100", base + "/job5-top100");
            langOutputs.put(lang, stages);
        }
        return langOutputs;
    }

    private static List<StepConfig> buildSteps(String jarS3Uri, Map<String, Map<String, String>> outputs, CliArgs cli) {
        List<StepConfig> steps = new ArrayList<>();
        for (String lang : LANG_ORDER) {
            if (!cli.runLangs.contains(lang)) {
                continue;
            }
            String input = "he".equals(lang) ? HEB_INPUT : ENG_INPUT;
            String stopwords = "he".equals(lang) ? cli.stopwordsHe : cli.stopwordsEn;
            Map<String, String> stagePaths = outputs.get(lang);

            if ("both".equalsIgnoreCase(cli.countsMode)) {
                steps.add(createStepConfig(lang + "-job1-counts-nc", jarS3Uri, JOB1_MAIN,
                        Arrays.asList(input, stagePaths.get("job1-counts-nc"), lang, stopwords, "--useCombiner=false", "-Dcolloc.debugTypes=true")));
            }
            steps.add(createStepConfig(lang + "-job1-counts-c", jarS3Uri, JOB1_MAIN,
                    Arrays.asList(input, stagePaths.get("job1-counts-c"), lang, stopwords, "--useCombiner=true", "-Dcolloc.debugTypes=true")));
            steps.add(createStepConfig(lang + "-job2-join1", jarS3Uri, JOB2_MAIN,
                    Arrays.asList(stagePaths.get("job1-counts-c"), stagePaths.get("job2-join1"))));
            steps.add(createStepConfig(lang + "-job3-join2", jarS3Uri, JOB3_MAIN,
                    Arrays.asList(stagePaths.get("job2-join1"), stagePaths.get("job1-counts-c"), stagePaths.get("job3-join2"))));
            steps.add(createStepConfig(lang + "-job4-llr", jarS3Uri, JOB4_MAIN,
                    Arrays.asList(stagePaths.get("job3-join2"), stagePaths.get("job1-counts-c"), stagePaths.get("job4-llr"))));
            steps.add(createStepConfig(lang + "-job5-top100", jarS3Uri, JOB5_MAIN,
                    Arrays.asList(stagePaths.get("job4-llr"), stagePaths.get("job5-top100"))));
        }
        return steps;
    }

    private static StepConfig createStepConfig(String name, String jarS3Uri, String mainClass, List<String> args) {
        HadoopJarStepConfig jarStep = new HadoopJarStepConfig()
                .withJar(jarS3Uri)
                .withMainClass(mainClass)
                .withArgs(args);
        return new StepConfig()
                .withName(name)
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(jarStep);
    }

    private static void printOutputPaths(Map<String, Map<String, String>> outputs) {
        System.out.println("Output paths:");
        for (Map.Entry<String, Map<String, String>> langEntry : outputs.entrySet()) {
            String lang = langEntry.getKey();
            System.out.println("  Language: " + lang);
            for (Map.Entry<String, String> stage : langEntry.getValue().entrySet()) {
                System.out.println(String.format("    %s -> %s", stage.getKey(), stage.getValue()));
            }
        }
    }

    private static void printStepCommands(String jarS3Uri, List<StepConfig> steps) {
        System.out.println("Steps to be submitted:");
        for (StepConfig step : steps) {
            HadoopJarStepConfig cfg = step.getHadoopJarStep();
            List<String> args = cfg.getArgs() == null ? new ArrayList<>() : cfg.getArgs();
            String outputHint = args.isEmpty() ? "" : args.get(args.size() - 1);
            String args0 = args.size() > 0 ? args.get(0) : "";
            String args1 = args.size() > 1 ? args.get(1) : "";
            String args2 = args.size() > 2 ? args.get(2) : "";
            System.out.println(String.format("SUBMIT_STEP name=%s jar=%s mainClass=%s args0=%s args1=%s args2=%s outputHint=%s",
                    step.getName(), cfg.getJar(), cfg.getMainClass(), args0, args1, args2, outputHint));
        }
    }

    private static S3UriParts parseS3Uri(String uri) {
        if (uri == null || !uri.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
        }
        String trimmed = uri.substring("s3://".length());
        int slash = trimmed.indexOf('/');
        if (slash <= 0 || slash == trimmed.length() - 1) {
            throw new IllegalArgumentException("S3 URI must include bucket and key: " + uri);
        }
        String bucket = trimmed.substring(0, slash);
        String key = trimmed.substring(slash + 1);
        return new S3UriParts(bucket, key);
    }

    private static class S3UriParts {
        final String bucket;
        final String key;

        S3UriParts(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }

    private static class CliArgs {
        String jarLocalPath;
        String jarS3Uri;
        String bucket;
        String logUri;
        String keyName;
        String instanceType = "m4.large";
        int instanceCount = 2;
        List<String> runLangs = new ArrayList<>(LANG_ORDER);
        String stopwordsHe = "heb-stopwords.txt";
        String stopwordsEn = "eng-stopwords.txt";
        boolean forceLocal = false;
        String countsMode = "combinerOnly"; // combinerOnly | both

        static CliArgs parse(String[] rawArgs) {
            CliArgs parsed = new CliArgs();
            for (int i = 0; i < rawArgs.length; i++) {
                String arg = rawArgs[i];
                switch (arg) {
                    case "--jar":
                        parsed.jarLocalPath = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--jarS3":
                        parsed.jarS3Uri = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--bucket":
                        parsed.bucket = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--logUri":
                        parsed.logUri = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--keyName":
                        parsed.keyName = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--instanceType":
                        parsed.instanceType = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--instanceCount":
                        parsed.instanceCount = Integer.parseInt(requireValue(arg, rawArgs, ++i));
                        break;
                    case "--runLangs":
                        parsed.runLangs = parseLanguages(requireValue(arg, rawArgs, ++i));
                        break;
                    case "--stopwordsHe":
                        parsed.stopwordsHe = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--stopwordsEn":
                        parsed.stopwordsEn = requireValue(arg, rawArgs, ++i);
                        break;
                    case "--forceLocal":
                        parsed.forceLocal = true;
                        break;
                    case "--countsMode":
                        parsed.countsMode = requireValue(arg, rawArgs, ++i);
                        break;
                    default:
                        usage("Unknown argument: " + arg);
                }
            }
            parsed.validate();
            return parsed;
        }

        private void validate() {
            if (jarLocalPath == null && jarS3Uri == null) {
                usage("Either --jar or --jarS3 is required.");
            }
            if (bucket == null || bucket.isEmpty()) {
                usage("--bucket is required.");
            }
            if (logUri == null || logUri.isEmpty()) {
                usage("--logUri is required.");
            }
            if (instanceCount < 2) {
                instanceCount = 2;
            }
            if (runLangs.isEmpty()) {
                runLangs = new ArrayList<>(LANG_ORDER);
            }
            if (!"combinerOnly".equalsIgnoreCase(countsMode) && !"both".equalsIgnoreCase(countsMode)) {
                usage("Invalid --countsMode. Use 'combinerOnly' or 'both'.");
            }
        }

        private static List<String> parseLanguages(String csv) {
            String[] parts = csv.split(",");
            Set<String> langs = new LinkedHashSet<>();
            for (String p : parts) {
                String normalized = p.trim().toLowerCase(Locale.ROOT);
                if (normalized.equals("he") || normalized.equals("en")) {
                    langs.add(normalized);
                }
            }
            List<String> ordered = new ArrayList<>();
            for (String lang : LANG_ORDER) {
                if (langs.contains(lang)) {
                    ordered.add(lang);
                }
            }
            return ordered;
        }

        private static String requireValue(String flag, String[] args, int index) {
            if (index >= args.length) {
                usage("Missing value for " + flag);
            }
            return args[index];
        }

        private static void usage(String message) {
            System.err.println(message);
            System.err.println("Usage: java -cp <jar> collocations.EmrFlowRunner "
                    + "--jar <localJar> OR --jarS3 <s3Uri> "
                    + "--bucket <outputBucket> --logUri <s3LogUri> "
                    + "[--keyName <ec2Key>] [--instanceType <m4.large>] [--instanceCount <2>] "
                    + "[--runLangs <he,en>] [--stopwordsHe <heb-stopwords.txt>] [--stopwordsEn <eng-stopwords.txt>] "
                    + "[--countsMode <combinerOnly|both>]");
            System.exit(1);
        }
    }
}
