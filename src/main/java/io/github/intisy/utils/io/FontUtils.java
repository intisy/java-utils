package io.github.intisy.utils.io;

import java.awt.*;
import java.io.File;
import java.io.IOException;

/**
 * Utility class for font-related operations.
 * This class provides methods for loading and manipulating fonts for use in Java applications.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class FontUtils {
    /**
     * Loads a TrueType font from a file and creates a Font object with the specified size.
     * The font is loaded with PLAIN style by default.
     *
     * @param fontPath the file path to the TrueType font file
     * @param fontSize the desired size of the font in points
     * @return a Font object created from the specified file with the specified size
     * @throws RuntimeException if the font file cannot be read or has an unsupported format
     */
    public static Font loadFont(File fontPath, int fontSize) {
        try {
            return Font.createFont(Font.TRUETYPE_FONT, fontPath).deriveFont(Font.PLAIN, fontSize);
        } catch (FontFormatException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
