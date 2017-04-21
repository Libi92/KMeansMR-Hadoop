/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kmeansmr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import kmeansmr.mapreduce.Constants;
import kmeansmr.mapreduce.KMeansMapper;
import kmeansmr.mapreduce.KMeansReducer;
import kmeansmr.utils.Vector;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author libin
 */
public class KMeansKDD {
    
    static String BASEURL = "/user/hue/KMeansMR/";
    static String INPUT = BASEURL + "kddinput/";
    static String OUTPUT = BASEURL + "kddoutput/";

    static String CENTER_DIR = BASEURL + "kddcenter/";
    static String CENTER_CONVERGED = BASEURL + "converged.txt";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        try {

            int iterations = 1;
            Path convergerPath = new Path(CENTER_CONVERGED);
            Path centerPath = new Path(CENTER_DIR + "centers.txt");
            Path nextCenterPath = new Path(CENTER_DIR + "centers" + (iterations + 1) + ".txt");

            JobConf conf = new JobConf(KMeansKDD.class);

            Path outPath = new Path(OUTPUT);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            if (fs.exists(convergerPath)) {
                fs.delete(convergerPath, true);
            }
            
            FileStatus[] fss = fs.listStatus(new Path(CENTER_DIR));
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (path.toString().contains("centers.txt")) {
                    continue;
                }
                fs.delete(path);
            }

            conf.setJobName("KMEANS_" + iterations);
            conf.setMapOutputKeyClass(IntWritable.class);
            conf.setMapOutputValueClass(Vector.class);
            conf.setOutputKeyClass(IntWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(KMeansMapper.class);
            conf.setReducerClass(KMeansReducer.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.set(Constants.CENTER, centerPath.toString());
            conf.set(Constants.NEXTCENTER, nextCenterPath.toString());
            conf.setInt(Constants.STARTINDEX, 5);
            conf.setInt(Constants.ENDINDEX, 41);
            conf.setInt(Constants.CLASSINDEX, 42);

            FileInputFormat.setInputPaths(conf, new Path(INPUT));
            FileOutputFormat.setOutputPath(conf, new Path(OUTPUT));

            JobClient.runJob(conf);

            while (true) {

                System.out.println("------CENTERS------");

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(nextCenterPath)));
                String line;
                line = br.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }
                iterations++;

                centerPath = new Path(CENTER_DIR + "centers" + iterations + ".txt");
                nextCenterPath = new Path(CENTER_DIR + "centers" + (iterations + 1) + ".txt");

                conf = new JobConf(KMeansIris.class);

                outPath = new Path(OUTPUT);
                fs = FileSystem.get(conf);
                if (fs.exists(outPath)) {
                    fs.delete(outPath, true);
                }

                conf.setJobName("KMEANS_" + iterations);
                conf.setMapOutputKeyClass(IntWritable.class);
                conf.setMapOutputValueClass(Vector.class);
                conf.setOutputKeyClass(IntWritable.class);
                conf.setOutputValueClass(Text.class);
                conf.setMapperClass(KMeansMapper.class);
                conf.setReducerClass(KMeansReducer.class);
                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);
                conf.set(Constants.CENTER, centerPath.toString());
                conf.set(Constants.NEXTCENTER, nextCenterPath.toString());
                conf.setInt(Constants.STARTINDEX, 5);
                conf.setInt(Constants.ENDINDEX, 41);
                conf.setInt(Constants.CLASSINDEX, 42);

                FileInputFormat.setInputPaths(conf, new Path(INPUT));
                FileOutputFormat.setOutputPath(conf, new Path(OUTPUT));

                JobClient.runJob(conf);
                
                if(fs.exists(convergerPath)){
                    break;
                }
            }

            fss = fs.listStatus(new Path(OUTPUT));
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (path.toString().contains("_SUCCESS")) {
                    continue;
                }

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                line = br.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }

            }

            System.out.println("------CENTERS------");

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centerPath)));
            String line;
            line = br.readLine();
            while (line != null) {
                System.out.println(line);
                line = br.readLine();
            }

        } catch (IOException ex) {
            Logger.getLogger(KMeansIris.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
