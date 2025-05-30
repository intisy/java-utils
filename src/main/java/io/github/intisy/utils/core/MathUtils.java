package io.github.intisy.utils.core;

/**
 * Utility class providing mathematical operations and array manipulations.
 * This class includes methods for array transformations, interpolation functions,
 * and easing functions commonly used in animations and transitions.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class MathUtils {
    /**
     * Multiplies the size of an array by the specified multiplier by repeatedly doubling it.
     * This method increases the array size by interpolating values between existing elements.
     *
     * @param array the array to multiply
     * @param multiplier the factor by which to multiply the array size
     * @return a new array with size approximately multiplier times the original
     */
    public static double[] multiplyArray(double[] array, int multiplier) {
        double[] newArray = array;
        for (int i = 1; i < multiplier; i++) {
            newArray = doubleArray(newArray);
        }
        return newArray;
    }

    /**
     * Doubles the size of an array by interpolating values between existing elements.
     * For each pair of adjacent elements in the original array, this method inserts
     * their average as a new element between them. The last element of the new array
     * is extrapolated based on the last two elements of the original array.
     *
     * @param array the array to double in size
     * @return a new array with twice the size of the original
     */
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

    /**
     * Reverses the order of elements in the given array in-place.
     * This method modifies the input array directly and also returns it for convenience.
     *
     * @param array the array to reverse
     * @return the same array with its elements in reverse order
     */
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
    /**
     * Creates an easing function array with a quadratic curve from 1 to 2 over 1.5 time units.
     * This method generates a 15-element array representing a quadratic ease-in curve.
     *
     * @return an array of 15 double values representing the easing function
     */
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

    /**
     * Creates an easing function array with a quadratic curve from 1 to 2 over 1.0 time units.
     * This method generates a 10-element array representing a quadratic ease-in curve.
     *
     * @return an array of 10 double values representing the easing function
     */
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

    /**
     * Creates an easing function array with a quadratic curve from 0 to 45 over 1.5 time units.
     * This method generates a 15-element array representing a quadratic ease-in curve.
     *
     * @return an array of 15 double values representing the easing function
     */
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

    /**
     * Creates an easing function array with a quadratic curve from 0 to 45 over 1.0 time units.
     * This method generates a 10-element array representing a quadratic ease-in curve.
     *
     * @return an array of 10 double values representing the easing function
     */
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