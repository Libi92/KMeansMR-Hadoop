/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kmeansmr;

import java.util.Scanner;

/**
 *
 * @author libin
 */
public class Application {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("MapReduce KMeans Clusterer");
        
        OUTER:            
        while (true) {
            System.out.println("1. Iris");
            System.out.println("2. KDD Cup");
            System.out.println("3. Story");
            String option = scanner.nextLine();
            
            switch (option) {
                case "1":
                    KMeansIris.main(args);
                    break;
                case "2":
                    KMeansKDD.main(args);
                    break;
                case "3":
                    break OUTER;
                default:
                    break;
            }
        }
    }
}
