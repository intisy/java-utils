package io.github.intisy.utils.utils;

import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;

public class ClipboardUtils {
    /**
 * Copies the given text to the system clipboard.
 *
 * @param text The text to be copied to the clipboard.
 *
 * @throws SecurityException If a security manager exists and its
 *         {@code checkPermission} method doesn't allow
 *         {@code AWTPermission("accessClipboard")} to be granted.
 * @throws IllegalStateException If the system clipboard is unavailable.
 * @throws NullPointerException If the given text is {@code null}.
 */
public static void copyToClipboard(String text) {
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection selection = new StringSelection(text);
    clipboard.setContents(selection, null);
}
}
