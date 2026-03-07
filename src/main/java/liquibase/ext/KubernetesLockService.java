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
                String podNamespace = null;
                String podName = null;

                StringTokenizer tok = new StringTokenizer(lockedBy, ":");
                if (tok.countTokens() == 2) {
                    podNamespace = tok.nextToken();
                    podName = tok.nextToken();
                } else if (lockedBy.contains(" (") && lockedBy.endsWith(")")) {
                    podName = lockedBy.substring(0, lockedBy.indexOf(" ("));
                    podNamespace = KubernetesConnector.getInstance().getPodNamespace();
                }

                if (podName != null && podNamespace != null) {
                    if (KubernetesConnector.getInstance().isCurrentPod(podNamespace, podName)) {
                        LOG.info("Lock created by the same pod, release lock");
                        releaseLock();
                    } else if (!KubernetesConnector.getInstance().isPodActive(podNamespace, podName)) {
                        LOG.info("Lock created by an inactive pod, release lock");
                        releaseLock();
                    }
                } else {
                    LOG.warn("Can't parse LOCKEDBY field: {}", lockedBy);
                }
            } else {
                LOG.info("Database is not locked");
            }
        } catch (DatabaseException e) {
            LOG.error("Can't read the LOCKEDBY field from databasechangeloglock", e);
        } catch (Exception e) {
            LOG.error("Error checking lock status", e);
        }
        super.waitForLock();
    }
}


