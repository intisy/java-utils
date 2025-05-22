package io.github.intisy.utils.core;

import java.nio.ShortBuffer;

/**
 * Utility class containing methods for working with instances of {@link ShortBuffer}.
 * This class provides various operations such as printing buffer contents, combining buffers,
 * finding middle values, and performing arithmetic operations on buffer elements.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class BufferUtils {
    /**
     * Prints the contents of a ShortBuffer to the standard output. The elements
     * of the buffer are printed sequentially, separated by spaces. After printing,
     * the buffer is flipped for potential further processing.
     *
     * @param buffer the ShortBuffer whose contents are to be printed
     */
    public static void printBuffer(ShortBuffer buffer) {
        System.out.print("Buffer content: ");
        while (buffer.hasRemaining()) {
            System.out.print(buffer.get() + " ");
        }
        System.out.println();
        buffer.flip();
    }

    /**
     * Calculates the element-wise middle value between two given ShortBuffers and
     * returns a new ShortBuffer containing these values. The middle value for each
     * corresponding pair of elements is calculated as the average of the two values.
     * If one buffer runs out of elements, the remaining elements from the other buffer
     * are used as is.
     *
     * @param buffer1 the first ShortBuffer to compare, must not be null
     * @param buffer2 the second ShortBuffer to compare, must not be null
     * @return a new ShortBuffer containing the middle values of the corresponding
     *         elements in the input buffers
     */
    public static ShortBuffer findMiddleValue(ShortBuffer buffer1, ShortBuffer buffer2) {
        int writeCount = Math.min(buffer1.remaining(), buffer2.remaining());

        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);

        for (int i = 0; i < writeCount; i++) {
            short value1 = buffer1.hasRemaining() ? buffer1.get() : 0;
            short value2 = buffer2.hasRemaining() ? buffer2.get() : value1;
            if(value1 == 0)value1=value2;
            short middleValue = (short) ((value1 + value2) / 2);
            middleBuffer.put(middleValue);
        }

        middleBuffer.flip();
        buffer1.flip();
        buffer2.flip();
        return middleBuffer;
    }

    /**
     * Combines the values of two ShortBuffers element-wise. If both buffers have remaining elements, the
     * corresponding elements are added together and stored in a new buffer. If one buffer has remaining
     * elements after the other is exhausted, the remaining elements are appended to the resulting buffer.
     * The resulting buffer's position is reset to zero before returning.
     *
     * @param buffer1 the first ShortBuffer to combine, must not be null
     * @param buffer2 the second ShortBuffer to combine, must not be null
     * @return a new ShortBuffer containing the combined values of the input buffers
     */
    public static ShortBuffer combineValue(ShortBuffer buffer1, ShortBuffer buffer2) {
        int combinedSize = buffer1.remaining() + buffer2.remaining();
        ShortBuffer combinedBuffer = ShortBuffer.allocate(combinedSize);

        while (buffer1.hasRemaining() && buffer2.hasRemaining()) {
            short value1 = buffer1.get();
            short value2 = buffer2.get();
            combinedBuffer.put((short) (value1 + value2));
        }

        while (buffer1.hasRemaining()) {
            combinedBuffer.put(buffer1.get());
        }

        while (buffer2.hasRemaining()) {
            combinedBuffer.put(buffer2.get());
        }

        combinedBuffer.flip();
        return combinedBuffer;
    }

    /**
     * Divides each element in the given ShortBuffer by a specified factor and returns a new ShortBuffer
     * containing the resulting values. The input buffer is consumed during the operation, and the
     * resulting buffer's position is reset for reading.
     *
     * @param buffer the ShortBuffer to process, must not be null
     * @param factor the factor by which to divide each element
     * @return a new ShortBuffer containing the scaled values
     */
    public static ShortBuffer divideBuffer(ShortBuffer buffer, double factor) {
        int writeCount = buffer.remaining();
        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);
        for (int i = 0; i < writeCount; i++) {
            short value = (short) (buffer.get() * factor);
            middleBuffer.put(value);
        }
        middleBuffer.flip();
        return middleBuffer;
    }

    /**
     * Multiplies each element in the given ShortBuffer by a specified factor and returns a new ShortBuffer
     * containing the resulting values. The input buffer is consumed during the operation, and the
     * resulting buffer's position is reset for reading.
     *
     * @param buffer the ShortBuffer to process, must not be null
     * @param factor the factor by which to multiply each element
     * @return a new ShortBuffer containing the scaled values
     */
    public static ShortBuffer multiplyBuffer(ShortBuffer buffer, double factor) {
        int writeCount = buffer.remaining();
        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);
        for (int i = 0; i < writeCount; i++) {
            double value = buffer.get() * factor;
            value = Math.min(Short.MAX_VALUE, Math.max(Short.MIN_VALUE, value));
            middleBuffer.put((short) value);
        }
        middleBuffer.flip();
        return middleBuffer;
    }
}
