package io.github.intisy.utils.api;

import io.github.intisy.simple.logger.StaticLogger;
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

@SuppressWarnings("unused")
public class Git {
    private String apiKey;
    private String repoName;
    private String repoOwner;
    private final File path;
    public Git(File path) {
        this.path = path;
    }
    public Git(String repoOwner, String repoName, String apiKey, File path) {
        this.apiKey = apiKey;
        this.repoName = repoName;
        this.repoOwner = repoOwner;
        this.path = path;
    }
    public GitHub getGitHub() {
        return new GitHub(repoOwner, repoName, apiKey, true);
    }
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
            StaticLogger.exception(exception);
        }
        return changes;
    }
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
    public void cloneRepository() throws GitAPIException {
        String repositoryURL = "https://github.com/" + repoOwner + "/" + repoName;
        StaticLogger.note("Cloning repository... (" + repositoryURL + ")");
        CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(repoOwner, apiKey);
        try (org. eclipse. jgit. api. Git git = org.eclipse.jgit.api.Git.cloneRepository()
                .setURI(repositoryURL)
                .setCredentialsProvider(credentialsProvider)
                .setDirectory(path)
                .call()) {
            StaticLogger.success("Repository cloned successfully.");
        }
    }
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
            StaticLogger.exception(exception);
        }
        return false;
    }

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
            StaticLogger.exception(exception);
            return false;
        }
    }

    public void pullRepository() throws GitAPIException, IOException {
        CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(repoOwner, apiKey);
        try (org.eclipse.jgit.api.Git repo = org.eclipse.jgit.api.Git.open(path)) {
            Repository repository = repo.getRepository();
            org.eclipse.jgit.api.Git git = new org.eclipse.jgit.api.Git(repository);
            git.fetch().setCredentialsProvider(credentialsProvider).call();
            List<Ref> branches = git.branchList().call();
            if (branches.size() > 1) {
                StaticLogger.warning("Repository has multiple branches, might pull wrong branch...");
                for (Ref branch : branches) {
                    StaticLogger.warning("Branch: " + branch.getName());
                }
            }
            PullCommand pullCmd = git.pull()
                    .setCredentialsProvider(new UsernamePasswordCredentialsProvider(repoOwner, apiKey))
                    .setRemoteBranchName(branches.get(0).getName());
            StaticLogger.note("Pulling Repository branch" + branches.get(0).getName());
            PullResult result = pullCmd.call();
            if (!result.isSuccessful()) {
                StaticLogger.error("Pull failed: " + branches.get(0).getName());
            } else {
                StaticLogger.success("Successfully pulled repository.");
            }
        }
    }
    public void cloneOrPullRepository() throws GitAPIException, IOException {
        if (doesRepoExist()) {
            if (!isRepoUpToDate())
                pullRepository();
            else {
                StaticLogger.note("Repository is up to date.");
            }
        } else {
            cloneRepository();
        }
    }
}
