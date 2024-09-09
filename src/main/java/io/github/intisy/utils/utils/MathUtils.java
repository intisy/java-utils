package io.github.intisy.utils.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MathUtils {
    public static double[] multiplyArray(double[] array, int multiplier) {
        double[] newArray = array;
        for (int i = 1; i < multiplier; i++) {
            newArray = doubleArray(newArray);
        }
        return newArray;
    }
    public static double[] doubleArray(double[] array) {
        double[] newArray = new double[array.length*2];
        // Fill in the extra values with the average of the surrounding values
        for (int i = 0; i < array.length; i++) {
            if (i < array.length-1) {
                newArray[i * 2 + 1] = (array[i] + array[i + 1]) / 2.0;
            }
            newArray[i*2] = array[i];
        }
        newArray[array.length*2-1] = array[array.length-1]-(array[array.length-2]-array[array.length-1])/2;
        return newArray;
    }
    public static double[] reverseArray(double[] array) {
        int left = 0;
        int right = array.length - 1;

        // Swap elements from left and right ends
        while (left < right) {
            // Swap array[left] and array[right]
            double temp = array[left];
            array[left] = array[right];
            array[right] = temp;

            // Move to next pair
            left++;
            right--;
        }
        return array;
    }
    public static double[] easeIn3() {
        double endTime = 1.5;
        double[] array = new double[15];

        // Interpolate over time
        for (int t = 0; (double) t /10 < endTime; t += 1) {
            array[t] = 1+ ((double) t / 10 / endTime)*((double) t / 10 / endTime);
        }
        array[14] = 2;
        return array;
    }
    public static double[] easeIn2() {
        double endTime = 1.0;
        double[] array = new double[10];

        // Interpolate over time
        for (int t = 0; (double) t /10 < endTime; t += 1) {
            array[t] = 1+ ((double) t / 10 / endTime)*((double) t / 10 / endTime);
        }
        array[9] = 2;
        return array;
    }
    public static double[] easeIn4() {
        double endTime = 1.5;
        double[] array = new double[15];

        // Interpolate over time
        for (int t = 0; (double) t /10 < endTime; t += 1) {
            array[t] = ((double) t / 15 / endTime)*((double) t / 15 / endTime)*45;
        }
        array[14] = 45;
        return array;
    }
    public static double[] easeIn() {
        double endTime = 1.0;
        double[] array = new double[10];

        // Interpolate over time
        for (int t = 0; (double) t /10 < endTime; t += 1) {
            array[t] = ((double) t / 10 / endTime)*((double) t / 10 / endTime)*45;
        }
        array[9] = 45;
        return array;
    }
}
