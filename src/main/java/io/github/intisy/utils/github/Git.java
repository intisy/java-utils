package io.github.intisy.utils.github;

import io.github.intisy.simple.logger.Log;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * A utility class for Git operations using JGit.
 * This class provides methods for common Git operations such as cloning repositories,
 * pulling updates, pushing changes, and checking repository status.
 * It can be used to interact with GitHub repositories programmatically.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class Git {
    /**
     * The API key or personal access token for GitHub authentication.
     */
    private String apiKey;

    /**
     * The name of the repository.
     */
    private String repoName;

    /**
     * The owner (user or organization) of the repository.
     */
    private String repoOwner;

    /**
     * The local file path where the repository is or will be located.
     */
    private final File path;

    /**
     * Constructs a Git instance with only a local path.
     * This constructor is useful when working with existing local repositories
     * where remote operations are not needed.
     *
     * @param path the local file path of the repository
     */
    public Git(File path) {
        this.path = path;
    }

    /**
     * Constructs a Git instance with repository details and authentication.
     * This constructor provides all the information needed for remote operations.
     *
     * @param repoOwner the owner (user or organization) of the repository
     * @param repoName the name of the repository
     * @param apiKey the API key or personal access token for GitHub authentication
     * @param path the local file path where the repository is or will be located
     */
    public Git(String repoOwner, String repoName, String apiKey, File path) {
        this.apiKey = apiKey;
        this.repoName = repoName;
        this.repoOwner = repoOwner;
        this.path = path;
    }

    /**
     * Creates a GitHub API wrapper for this repository.
     * This method provides access to GitHub-specific operations.
     *
     * @return a GitHub instance configured for this repository
     */
    public GitHub getGitHub() {
        return new GitHub(repoOwner, repoName, apiKey, true);
    }
    /**
     * Gets all changes in the local repository compared to the last commit.
     * This method categorizes changes into added, modified, deleted, and untracked files.
     *
     * @return a map containing sets of file paths categorized by change type
     */
    public Map<String, Set<String>> getAllChanges() {
        Map<String, Set<String>> changes = new HashMap<>();
        changes.put("added", new HashSet<>());
        changes.put("modified", new HashSet<>());
        changes.put("deleted", new HashSet<>());
        changes.put("untracked", new HashSet<>());
        try {
            org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.open(path);
            Status status = git.status().call();
            changes.get("added").addAll(status.getAdded());
            changes.get("added").addAll(status.getUntracked());
            changes.get("modified").addAll(status.getModified());
            changes.get("deleted").addAll(status.getRemoved());
            changes.get("deleted").addAll(status.getMissing());

            git.close();
        } catch (IOException | GitAPIException exception) {
            Log.error(exception);
        }
        return changes;
    }

    /**
     * Gets the SHA-1 hash of the most recent commit in the repository.
     *
     * @return the SHA-1 hash of the most recent commit, or null if an error occurs
     */
    public String getCurrentSha() {
        try {
            org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.open(path);
            Iterable<RevCommit> commits = git.log().setMaxCount(1).call();
            for (RevCommit commit : commits) {
                return commit.getName();
            }
            git.close();
        } catch (Exception ignored) {}
        return null;
    }
    /**
     * Clones the repository from GitHub to the local path.
     * This method uses the repository owner, name, and API key to authenticate
     * and clone the repository to the specified path.
     *
     * @throws GitAPIException if an error occurs during the clone operation
     */
    public void cloneRepository() throws GitAPIException {
        String repositoryURL = "https://github.com/" + repoOwner + "/" + repoName;
        Log.note("Cloning repository... (" + repositoryURL + ")");
        CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(repoOwner, apiKey);
        try (org. eclipse. jgit. api. Git git = org.eclipse.jgit.api.Git.cloneRepository()
                .setURI(repositoryURL)
                .setCredentialsProvider(credentialsProvider)
                .setDirectory(path)
                .call()) {
            Log.success("Repository cloned successfully.");
        }
    }

    /**
     * Pushes changes to a file to the remote repository.
     * This method adds or removes the specified file, commits the changes with the given message,
     * and pushes the commit to the remote repository.
     *
     * @param filePath the path of the file to push, relative to the repository root
     * @param commitMessage the commit message
     * @return true if the push was successful, false otherwise
     */
    public boolean pushRepository(String filePath, String commitMessage) {
        CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(repoOwner, apiKey);
        try {
            org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.open(path);
            File file = new File(path, filePath);
            if (file.exists()) {
                git.add().addFilepattern(filePath).call();
            } else {
                git.rm().addFilepattern(filePath).call();
            }
            git.commit().setMessage(commitMessage).call();
            git.push()
                    .setCredentialsProvider(credentialsProvider)
                    .setRemote(Constants.DEFAULT_REMOTE_NAME)
                    .call();
            git.close();
            return true;

        } catch (IOException | GitAPIException exception) {
            Log.error(exception);
        }
        return false;
    }

    /**
     * Checks if a Git repository exists at the specified path.
     * This method verifies that the path contains a valid Git repository
     * by checking for the existence of the object database.
     *
     * @return true if a Git repository exists at the path, false otherwise
     */
    public boolean doesRepoExist() {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        try (Repository repository = builder.setGitDir(path.toPath().resolve(".git").toFile())
                .readEnvironment() // scan environment GIT_* variables
                .findGitDir() // scan up the file system tree
                .build()) {
            return repository.getObjectDatabase().exists();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Checks if the local repository is up to date with the remote repository.
     * This method fetches the latest changes from the remote repository and
     * compares the local and remote commit IDs of the current branch.
     *
     * @return true if the local repository is up to date, false otherwise
     */
    public boolean isRepoUpToDate() {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        try (Repository repository = builder.setGitDir(path.toPath().resolve(".git").toFile())
                .readEnvironment()
                .findGitDir()
                .build();
             org.eclipse.jgit.api.Git git = new org.eclipse.jgit.api.Git(repository)) {
            git.fetch().call();
            String branch = repository.getBranch();
            ObjectId localCommit = repository.resolve("refs/heads/" + branch);
            ObjectId remoteCommit = repository.resolve("refs/remotes/origin/" + branch);
            if (localCommit != null && remoteCommit != null) {
                return localCommit.equals(remoteCommit);
            } else {
                return false;
            }
        } catch (IOException | GitAPIException exception) {
            Log.error(exception);
            return false;
        }
    }

    /**
     * Pulls the latest changes from the remote repository.
     * This method fetches the latest changes from the remote repository and
     * merges them into the local repository. If the repository has multiple branches,
     * it will log a warning and attempt to pull the first branch.
     *
     * @throws GitAPIException if an error occurs during the Git operation
     * @throws IOException if an I/O error occurs
     */
    public void pullRepository() throws GitAPIException, IOException {
        CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(repoOwner, apiKey);
        try (org.eclipse.jgit.api.Git repo = org.eclipse.jgit.api.Git.open(path)) {
            Repository repository = repo.getRepository();
            org.eclipse.jgit.api.Git git = new org.eclipse.jgit.api.Git(repository);
            git.fetch().setCredentialsProvider(credentialsProvider).call();
            List<Ref> branches = git.branchList().call();
            if (branches.size() > 1) {
                Log.warning("Repository has multiple branches, might pull wrong branch...");
                for (Ref branch : branches) {
                    Log.warning("Branch: " + branch.getName());
                }
            }
            PullCommand pullCmd = git.pull()
                    .setCredentialsProvider(new UsernamePasswordCredentialsProvider(repoOwner, apiKey))
                    .setRemoteBranchName(branches.get(0).getName());
            Log.note("Pulling Repository branch" + branches.get(0).getName());
            PullResult result = pullCmd.call();
            if (!result.isSuccessful()) {
                Log.error("Pull failed: " + branches.get(0).getName());
            } else {
                Log.success("Successfully pulled repository.");
            }
        }
    }

    /**
     * Clones or pulls the repository depending on its current state.
     * If the repository already exists locally, this method checks if it's up to date
     * and pulls changes if needed. If the repository doesn't exist locally, it clones it.
     *
     * @throws GitAPIException if an error occurs during the Git operation
     * @throws IOException if an I/O error occurs
     */
    public void cloneOrPullRepository() throws GitAPIException, IOException {
        if (doesRepoExist()) {
            if (!isRepoUpToDate())
                pullRepository();
            else {
                Log.note("Repository is up to date.");
            }
        } else {
            cloneRepository();
        }
    }
}
