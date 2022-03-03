package liquibase.ext;


import com.github.patricio78.liquibase.kubernetes.KubernetesConnector;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class LockDatabaseChangeLogGeneratorKubernetes extends LockDatabaseChangeLogGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(LockDatabaseChangeLogGeneratorKubernetes.class);

    @Override
    public int getPriority() {
        return 1000;
    }

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return KubernetesConnector.getInstance().isConnected();
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();

        UpdateStatement updateStatement = new UpdateStatement(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName());
        updateStatement.addNewColumnValue("LOCKED", true);
        updateStatement.addNewColumnValue("LOCKGRANTED", new Timestamp(new java.util.Date().getTime()));
        final String lockedBy = String.format("%s:%s", KubernetesConnector.getInstance().getPodNamespace(), KubernetesConnector.getInstance().getPodName());
        updateStatement.addNewColumnValue("LOCKEDBY", lockedBy);
        updateStatement.setWhereClause(database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogTableName(), "ID") + " = 1 AND " + database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogTableName(), "LOCKED") + " = " + DataTypeFactory.getInstance().fromDescription("boolean", database).objectToSql(false, database));

        LOG.info("Generating Sql to lock database lockedBy : '{}'", lockedBy);

        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
    }
}