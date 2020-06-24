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
public class K_DIS {
    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

        private List<String> rk_file = new ArrayList<String>();
        public void setup(Context context) {
            try{
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

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("p"), new Text(value));
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
            String line = rk_file.get(0);
            String[] rk = line.split(",");
            double radius = Double.parseDouble(rk[0]);
            //int k = Integer.parseInt(rk[1]);
            int k=2;
            List<String> all_points= new ArrayList<>();

            // collect all data in one area
            for (Text value : values) {

                all_points.add(value.toString());

            }


            double[][] all_dists = new double[all_points.size()][all_points.size()];

            for (int i=0; i<all_points.size(); i++){
                String p1 = all_points.get(i);
                double[] xy1 = StringsToDoubles(p1.split(","));
                for (int j=0; j<all_points.size(); j++){
                    String p2 = all_points.get(j);
                    double[] xy2 = StringsToDoubles(p2.split(","));
                    double tmp=0;
                    for (int dim=0; dim < xy1.length; dim++){ tmp = tmp + (xy1[dim]-xy2[dim])*(xy1[dim]-xy2[dim]);}
                    all_dists[i][j] = Math.sqrt(tmp);
                }
            }

            int[][] dists_sort = new int[all_points.size()][all_points.size()];

            for (int i=0; i<all_points.size(); i++){
                int[] tmp = sortindex(all_dists[i]);
                for (int j=0; j<all_points.size(); j++){
                    dists_sort[i][j]=tmp[j];
                }
            }

            int[] Nreach = new int[all_points.size()];
            double[] Kdis = new double[all_points.size()];
            for (int i=0; i<all_points.size(); i++){
                int count=k;
                int cur_idex=0;
                int next_idex=0;
                for (int j=k; j<all_points.size()-1; j++){
                    cur_idex = dists_sort[i][j];
                    next_idex = dists_sort[i][j+1];
                    if (all_dists[i][next_idex]!=all_dists[i][cur_idex]){
                        break;
                    }else{
                        count++;
                    }

                }
                Kdis[i] = all_dists[i][cur_idex];
                Nreach[i] = count;
            }

/*
            for (int i=0; i<all_points.size(); i++){
                String tmp="";
                for (int j=0; j<all_points.size(); j++){
                    tmp+=String.valueOf(all_dists[i][j]);
                    tmp+=",";
                }
                tmp+="----";
                for (int j=0; j<all_points.size(); j++){
                    tmp+=String.valueOf(dists_sort[i][j]);
                    tmp+=",";
                }
                tmp+="----";
                tmp+=String.valueOf(Nreach[i]);
                tmp+="----";
                tmp+=String.valueOf(Kdis[i]);
                Text kk = new Text(all_points.get(i)+"----");
                Text vv = new Text(tmp);
                context.write(kk,vv);
            }

 */
            for (int i=0; i<all_points.size(); i++) {
                for (int j = 1; j <= Nreach[i]; j++) {
                    int nei_idx = dists_sort[i][j];
                    String tmp = "";
                    tmp += String.valueOf(all_points.get(nei_idx));
                    tmp += "--";
                    tmp += String.valueOf(Kdis[nei_idx]);
                    tmp += "--";
                    tmp += String.valueOf(all_dists[i][nei_idx]);
                    tmp += "--";
                    tmp += String.valueOf(Nreach[i]);
                    Text kk = new Text(all_points.get(i)+"--");
                    Text vv = new Text(tmp);
                    context.write(kk, vv);
                }
            }

        }
    }
    

    public static void main(String[] args) throws Exception{
        String inputPath = args[0]+"datasetP.csv";
        String outputPath = args[1];
        String radius = args[2];
        String threshold = args[3];
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "K_DIS");


        String path1 = "./input/r_k.csv";
        File file1 = new File(path1);
        FileWriter fwPoint = new FileWriter(file1.getAbsoluteFile());
        BufferedWriter bwPoint = new BufferedWriter(fwPoint);
        bwPoint.write(radius+","+threshold);
        bwPoint.flush();
        fwPoint.close();

        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(K_DIS.class);
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