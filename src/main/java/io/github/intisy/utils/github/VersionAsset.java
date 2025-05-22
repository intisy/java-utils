package io.github.intisy.utils.github;

import org.kohsuke.github.GHAsset;

/**
 * A class representing a GitHub release asset with its associated version.
 * This class pairs a version string with a GitHub asset object, making it easier
 * to track and manage versioned assets from GitHub releases.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class VersionAsset {
    /**
     * The version string associated with the asset.
     */
    String version;

    /**
     * The GitHub asset object.
     */
    GHAsset asset;

    /**
     * Constructs a new VersionAsset with the specified version and GitHub asset.
     *
     * @param key the version string
     * @param asset the GitHub asset
     */
    public VersionAsset(String key, GHAsset asset) {
        this.version = key;
        this.asset = asset;
    }

    /**
     * Returns the version string associated with this asset.
     *
     * @return the version string
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version string for this asset.
     *
     * @param version the new version string
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns the GitHub asset object.
     *
     * @return the GitHub asset
     */
    public GHAsset getAsset() {
        return asset;
    }

    /**
     * Sets the GitHub asset object.
     *
     * @param value1 the new GitHub asset
     */
    public void setValue1(GHAsset value1) {
        this.asset = value1;
    }

    /**
     * Returns a string representation of this VersionAsset.
     * The string representation includes the version and asset information.
     *
     * @return a string representation of this VersionAsset
     */
    @Override
    public String toString() {
        return "Triplet{" +
                "key=" + version +
                ", value=" + asset +
                '}';
    }
}
