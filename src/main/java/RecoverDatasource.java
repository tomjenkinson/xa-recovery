import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Test;
import org.postgresql.xa.PGXADataSource;

public class RecoverDatasource {

	@Test
	public void recover() throws SQLException, XAException {
		String hostName = "localhost";
		String userName = "sa";
		String password = "sa";
		String recoveryUserName = "sa";
		String recoveryPassword = "sa";
		String databaseName = "QUICKSTART_DATABASENAME";
		int port = 5432;

		{
			PGXADataSource dataSource = new PGXADataSource();
			dataSource.setPortNumber(port);
			dataSource.setUser(userName);
			dataSource.setPassword(password);
			dataSource.setServerName(hostName);
			dataSource.setDatabaseName(databaseName);

			XAConnection xaConnection = dataSource.getXAConnection();
			XAResource xaResource2 = xaConnection.getXAResource();
			Xid xid = new Xid() {

				public byte[] getBranchQualifier() {
					return "branch".getBytes();
				}

				public int getFormatId() {
					return 123456;
				}

				public byte[] getGlobalTransactionId() {
					return "gtrid".getBytes();
				}
			};

			xaResource2.start(xid, XAResource.TMNOFLAGS);
			xaConnection.getConnection().createStatement()
					.execute("INSERT INTO foo (bar) VALUES (1)");
			xaResource2.end(xid, XAResource.TMSUCCESS);
			xaResource2.prepare(xid);

		}

		{
			PGXADataSource dataSource = new PGXADataSource();
			dataSource.setPortNumber(port);
			dataSource.setUser(recoveryUserName);
			dataSource.setPassword(recoveryPassword);
			dataSource.setServerName(hostName);
			dataSource.setDatabaseName(databaseName);

			XAResource xaResource = dataSource.getXAConnection()
					.getXAResource();
			Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN);
			int rolledBack = 0;
			for (int i = 0; i < recover.length; i++) {
				try {
					System.out.println("Rolling back: " + recover[i]);
					xaResource.rollback(recover[i]);
					System.out.println("Rolled back");
					rolledBack++;

					xaResource.rollback(recover[i]);
					System.out.println("Rolled back");
					rolledBack++;
				} catch (XAException e) {
					if (e.errorCode == XAException.XAER_RMERR) {
						System.err
								.println("Receiced XAER_RMERR rather than XAER_NOTA");
					}
					System.err.println(e.errorCode);
					e.printStackTrace();
				}
			}
			xaResource.recover(XAResource.TMENDRSCAN);
			int expected = 1;
			if (expected >= 0) {
				assertTrue("Rolled back: " + rolledBack, rolledBack == expected);
			}
		}
	}
}
