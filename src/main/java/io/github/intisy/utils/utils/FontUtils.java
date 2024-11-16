package io.github.intisy.utils.utils;

import java.awt.*;
import java.io.File;
import java.io.IOException;

@SuppressWarnings("unused")
public class FontUtils {
    public static Font loadFont(File fontPath, int fontSize) {
        try {
            return Font.createFont(Font.TRUETYPE_FONT, fontPath).deriveFont(Font.PLAIN, fontSize);
        } catch (FontFormatException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
