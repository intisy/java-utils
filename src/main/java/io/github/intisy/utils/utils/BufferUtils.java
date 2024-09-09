package io.github.intisy.utils.utils;

import java.nio.ShortBuffer;

@SuppressWarnings("unused")
public class BufferUtils {
    public static void printBuffer(ShortBuffer buffer) {
        System.out.print("Buffer content: ");
        while (buffer.hasRemaining()) {
            System.out.print(buffer.get() + " ");
        }
        System.out.println();
        buffer.flip();
    }
    public static ShortBuffer findMiddleValue(ShortBuffer buffer1, ShortBuffer buffer2) {
        // Calculate the number of elements to be written to the middle buffer
        int writeCount = Math.min(buffer1.remaining(), buffer2.remaining());

        // Create a new buffer to hold the middle values
        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);

        // Write values from the larger buffer and calculate the middle value
        for (int i = 0; i < writeCount; i++) {
            short value1 = buffer1.hasRemaining() ? buffer1.get() : 0;
            short value2 = buffer2.hasRemaining() ? buffer2.get() : value1;
            if(value1 == 0)value1=value2;
            // Calculate the average of the middle values and write it to the middle buffer
            short middleValue = (short) ((value1 + value2) / 2);
            middleBuffer.put(middleValue);
        }

        middleBuffer.flip(); // Prepare the buffer for reading
        buffer1.flip(); // Prepare the buffer for reading
        buffer2.flip(); // Prepare the buffer for reading
        return middleBuffer;
    }
    public static ShortBuffer combineValue(ShortBuffer buffer1, ShortBuffer buffer2) {
        int combinedSize = buffer1.remaining() + buffer2.remaining();
        ShortBuffer combinedBuffer = ShortBuffer.allocate(combinedSize);

        while (buffer1.hasRemaining() && buffer2.hasRemaining()) {
            short value1 = buffer1.get();
            short value2 = buffer2.get();
            combinedBuffer.put((short) (value1 + value2));
        }

        // Append remaining elements from buffer1, if any
        while (buffer1.hasRemaining()) {
            combinedBuffer.put(buffer1.get());
        }

        // Append remaining elements from buffer2, if any
        while (buffer2.hasRemaining()) {
            combinedBuffer.put(buffer2.get());
        }

        combinedBuffer.flip(); // Prepare the buffer for reading
        return combinedBuffer;
    }
    public static ShortBuffer divideBuffer(ShortBuffer buffer, double factor) {
        int writeCount = buffer.remaining();
        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);
        for (int i = 0; i < writeCount; i++) {
            short value = (short) (buffer.get() * factor);
            middleBuffer.put(value);
        }
        middleBuffer.flip(); // Prepare the buffer for reading
        return middleBuffer;
    }
    public static ShortBuffer multiplyBuffer(ShortBuffer buffer, double factor) {
        int writeCount = buffer.remaining();
        ShortBuffer middleBuffer = ShortBuffer.allocate(writeCount);
        for (int i = 0; i < writeCount; i++) {
            double value = buffer.get() * factor;
            value = Math.min(Short.MAX_VALUE, Math.max(Short.MIN_VALUE, value));
            middleBuffer.put((short) value);
        }
        middleBuffer.flip(); // Prepare the buffer for reading
        return middleBuffer;
    }
}
