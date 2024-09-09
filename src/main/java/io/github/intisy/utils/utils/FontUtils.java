package io.github.intisy.utils.utils;

import java.awt.*;
import java.io.File;
import java.io.IOException;

public class FontUtils {
    public static Font loadFont(String fontPath, int fontSize) {
        try {
            return Font.createFont(Font.TRUETYPE_FONT, new File(fontPath)).deriveFont(Font.PLAIN, fontSize);
        } catch (FontFormatException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
