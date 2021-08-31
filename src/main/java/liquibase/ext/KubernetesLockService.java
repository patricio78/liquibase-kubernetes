package liquibase.ext;

import com.github.patricio78.liquibase.kubernetes.KubernetesConnector;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.lockservice.StandardLockService;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;

public class KubernetesLockService extends StandardLockService {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLockService.class);

    @Override
    public int getPriority() {
        return 1000;
    }

    @Override
    public boolean supports(Database database) {
        return KubernetesConnector.getInstance().isConnected();
    }

    @Override
    public void waitForLock() throws LockException {
        try {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",  database);
            String lockedBy = executor.queryForObject(new SelectFromDatabaseChangeLogLockStatement("LOCKEDBY"), String.class);
            if (StringUtils.isNotBlank(lockedBy)) {
                LOG.info("Database locked by: {}", lockedBy);
                StringTokenizer tok = new StringTokenizer(lockedBy, ":");
                if (tok.countTokens() == 2) {
                    String podNamespace = tok.nextToken();
                    String podName = tok.nextToken();
                    if (KubernetesConnector.getInstance().isCurrentPod(podNamespace, podName)) {
                        LOG.info("Lock created by the same pod, release lock");
                        releaseLock();
                    }
                    boolean lockHolderPodActive = KubernetesConnector.getInstance().isPodActive(podNamespace, podName);
                    if (!lockHolderPodActive) {
                        LOG.info("Lock created by an inactive pod, release lock");
                        releaseLock();
                    }
                } else {
                    LOG.info("Can't parse LOCKEDBY field: {}", lockedBy);
                }
            } else {
                LOG.info("Databased is not locked");
            }
        } catch (DatabaseException e) {
            LOG.error("Can't read the LOCKEDBY field from databasechangeloglock", e);
        }
        super.waitForLock();
    }
}


