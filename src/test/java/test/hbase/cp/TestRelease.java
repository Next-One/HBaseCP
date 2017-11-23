package test.hbase.cp;

import org.apache.hadoop.hbase.client.Connection;

import com.wang.hbase.HBaseConnectionPool;
import com.wang.hbase.IConnectionPool;
import com.wang.utils.HBaseUtil;

/**
 * @author root 测试连接的释放
 */
public class TestRelease {
	
	public static void testRelease() {
		IConnectionPool pool = HBaseConnectionPool.getInstance();
		Connection conn = pool.getConnection();
		Connection conn2 = pool.getConnection();
		Connection conn3 = pool.getConnection();
		Connection conn4 = pool.getConnection();
		HBaseUtil.sleep(5000);
		pool.releaseConnection(conn);
		HBaseUtil.sleep(8000);
		pool.closeConnection(conn2);
		pool.releaseConnection(conn3);
		HBaseUtil.sleep(13000);
		pool.releaseConnection(conn4);
		HBaseUtil.sleep(30000);
	}

	public static void main(String[] args) {
		testRelease();
	}
}
