import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;
import org.eclipse.jdt.internal.compiler.ast.DoubleLiteral;

import java.io.*;
import java.security.Key;
import java.util.*;
import java.util.Comparator;
// The code is for Distance-Based Outlier Detection Clustering. If a point with lesss than k points that are within r distance,
// then report the point as an outlier.
// The setUp part is used to define the r - radius and k - counts.
// The mapper split the dataset into n*n blocks, and distinguish the zone or supporting area of points.
// The reducer then start calculating the distances between points and report outliers in each zone.
// The code is worked by Nai-tan Chang.
public class LRD {
    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("--");
            Double Kdis = Double.parseDouble(line[2]);
            Double distop = Double.parseDouble(line[3]);
            int Nreach = Integer.parseInt(line[4]);

            String vv = String.valueOf(Math.max(Kdis,distop)/Nreach);
            context.write(new Text(line[0]), new Text(vv));
        }
    }

    public static class OutlierReducer extends Reducer<Text,Text,Text,Text> {
        private List<String> rk_file = new ArrayList<String>();

        public double[] StringsToDoubles(String[] strs){
            double[] res = new double[strs.length];
            for (int i=0; i<strs.length; i++){
                res[i] = Double.parseDouble(strs[i]);
            }
            return res;
        }

        public int[] sortindex(double[] dist){
            int[] res = new int[dist.length];
            Double[][] tmp = new Double[dist.length][2];
            for (int i = 0; i<dist.length; i++){
                tmp[i][0] = dist[i];
                tmp[i][1] = (double)i;
            }
            Comparator<Double[]> arrayComparator = new Comparator<Double[]>() {
                @Override
                public int compare(Double[] o1, Double[] o2) {
                    return o1[0].compareTo(o2[0]);
                }
            };
            Arrays.sort(tmp, arrayComparator);

            for (int i=0; i<dist.length; i++){
                double r = tmp[i][1];
                res[i] = (int)r;
            }
            return res;
        }

        public void setup(Reducer.Context context) {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    rk_file.clear();
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            rk_file.add(line);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double res = 0;
            for (Text value : values) {
                res += Double.parseDouble(value.toString());
            }

            Text vv = new Text(String.valueOf(1.0/res));
            context.write(key, vv);


        }
    }




    public static void main(String[] args) throws Exception{
        String inputPath = "output1/part-r-00000";
        String outputPath = "output2/";
        String radius = "2";
        String threshold = "4";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "LRD");


        String path1 = "./input/r_k.csv";
        File file1 = new File(path1);
        FileWriter fwPoint = new FileWriter(file1.getAbsoluteFile());
        BufferedWriter bwPoint = new BufferedWriter(fwPoint);
        bwPoint.write(radius+","+threshold);
        bwPoint.flush();
        fwPoint.close();

        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(LRD.class);
        job.setMapperClass(OutlierMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(OutlierReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}