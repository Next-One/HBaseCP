package test.hbase.cp;

import org.apache.hadoop.hbase.client.Connection;

import com.wang.hbase.HBaseConnectionPool;
import com.wang.hbase.IConnectionPool;
import com.wang.utils.HBaseUtil;

public class TestConnectionPool {
	public static void main(String[] args) throws InterruptedException {
		IConnectionPool pool = HBaseConnectionPool.getInstance();
		// 获取13个连接 会有3个线程处于等待状态
		for (int i = 0; i < 13; i++) {
			new getConnThread(pool).start();
		}
		HBaseUtil.sleep(1000 * 5);
		pool.shutdown();
		HBaseUtil.sleep(20000);

	}

}

/**
 * @author root 测试线程将获取connection
 */
class getConnThread extends Thread {
	IConnectionPool pool;

	public getConnThread(IConnectionPool pool) {
		this.pool = pool;
	}

	@Override
	public void run() {
		Connection conn = pool.getConnection();
		HBaseUtil.sleep(3000);
		System.out.println("i'm " + Thread.currentThread().getName());
		pool.releaseConnection(conn);
	}

}
