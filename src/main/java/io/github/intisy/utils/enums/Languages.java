package io.github.intisy.utils.enums;

import io.github.intisy.utils.custom.Triplet;

public enum Languages {
    ENGLISH(new Triplet<>("english", "en", "America/New_York")),
    FRENCH(new Triplet<>("french", "fr", "Europe/Paris")),
    GERMAN(new Triplet<>("german", "de", "Europe/Berlin")),
    ITALIAN(new Triplet<>("italian", "it", "Europe/Rome")),
    PORTUGUESE(new Triplet<>("portuguese", "pt", "Europe/Lisbon")),
    SPANISH(new Triplet<>("spanish", "es", "Europe/Madrid"));
    final Triplet<String, String, String> key;
    Languages(Triplet<String, String, String> key) {
        this.key = key;
    }
    public String getLanguage() {
        return key.getKey();
    }
    public String getCode() {
        return key.getValue1();
    }
    public String getTimezone() {
        return key.getValue2();
    }
    public static Languages getEnum(String value) {
        Class<Languages> languagesClass = Languages.class;
        for (Languages languages : languagesClass.getEnumConstants()) {
            if (languages.getCode().equalsIgnoreCase(value) || languages.getLanguage().equalsIgnoreCase(value) || languages.getTimezone().equalsIgnoreCase(value)) {
                return languages;
            }
        }
        throw new IllegalArgumentException("Unsupported language: " + value);
    }
}
