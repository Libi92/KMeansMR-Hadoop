/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kmeansmr.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author libin
 */
public class KDDGen {
    public static void main(String[] args) {
        BufferedReader br = null;
        try {
            File file = new File("/Users/libin/Documents/Projects/kmeansmapreduce/datasets/kddcup.data_10_percent");
//            File file1 = new File("/Users/libin/Documents/Projects/kmeansmapreduce/datasets/kddcup.data_10_percent_no");
            br = new BufferedReader(new FileReader(file));
//            PrintWriter pw = new PrintWriter(file1);
            
            String line = "";
            
            double[] minCenters = new double[41 - 5];
            double[] maxCenters = new double[41 - 5];
            
            for (int i = 0; i < minCenters.length; i++) {
                minCenters[i] = Double.MAX_VALUE;
            }
            
            for (int i = 0; i < maxCenters.length; i++) {
                maxCenters[i] = Double.MIN_VALUE;
            }
            
            int lineNo = 1;
            while ((line = br.readLine()) != null) {
                line = lineNo++ + "," + line;
                
                String[] split = line.split(",");
                
                for (int i = 5; i < 41; i++) {
                    if(minCenters[i - 5] > Double.parseDouble(split[i])){
                        minCenters[i - 5] = Double.parseDouble(split[i]);
                    }
                    
                    if(maxCenters[i - 5] < Double.parseDouble(split[i])){
                        maxCenters[i - 5] = Double.parseDouble(split[i]);
                    }
                    
                }
//                pw.println(line);
//                pw.flush();
            }
            
            String newLine = "";
            for (int i = 0; i < minCenters.length; i++) {
                newLine += minCenters[i] + ",";
            }
            System.out.println(newLine);
            
            newLine = "";
            String line4 = "";
            String line3 = "";
            String line2 = "";
            for (int i = 0; i < maxCenters.length; i++) {
                line4 += maxCenters[i] / 4 + ",";
                line3 += 3 *(maxCenters[i] / 4) + ",";
                line2 += maxCenters[i] / 2 + ",";
                newLine += maxCenters[i] + ",";
            }
            System.out.println(line2);
            System.out.println(line3);
            System.out.println(line4);
            System.out.println(newLine);
            
//            pw.close();
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(KDDGen.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(KDDGen.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(KDDGen.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
