package io.github.intisy.utils.custom;

import org.kohsuke.github.GHAsset;

@SuppressWarnings("unused")
public class VersionAsset {
    String version;
    GHAsset asset;
    public VersionAsset(String key, GHAsset asset) {
        this.version = key;
        this.asset = asset;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public GHAsset getAsset() {
        return asset;
    }

    public void setValue1(GHAsset value1) {
        this.asset = value1;
    }

    @Override
    public String toString() {
        return "Triplet{" +
                "key=" + version +
                ", value=" + asset +
                '}';
    }
}
