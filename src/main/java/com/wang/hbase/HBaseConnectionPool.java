package com.wang.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wang.utils.HBaseUtil;

/**
 * @author root HBase连接池类
 */
public class HBaseConnectionPool implements IConnectionPool {
	/**
	 * 添加slf4j日志实例对象
	 */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectionPool.class);
	/**
	 * 默认的初始连接数
	 */
	static final int INIT_CONNSUM = 3;

	/**
	 * cpu数量，线程数为cpu数量或者cpu + 1时性能最佳
	 */
	static final int NCPUS = Runtime.getRuntime().availableProcessors();
	/**
	 * 默认每过5分钟杀死一个空闲连接，如果超过最大空闲连接数的话
	 */
	static final int KILL_IDEL_CONN_COUNT = 10;
	/**
	 * 最大空闲连接数，超过这个时间 @KILL_IDEL_CONN_TIME，连接数会减一直到连接数合理
	 */
	static final int MAX_IDEL_CONNCUM = 5;
	/**
	 * 单个连接的最大使用次数
	 */
	static final int MAX_USAGE_COUNT = 20;

	/**
	 * 固定大小的线程池,这个可以给出去使用吗
	 */
	private final ExecutorService threadPool;

	/**
	 * 空闲连接集合
	 */
	private final BlockingDeque<ConnectionEntity> idel;
	/**
	 * 活跃连接集合
	 */
	private final BlockingDeque<ConnectionEntity> active;
	/**
	 * 初始连接数
	 */
	private final int initSize;

	/**
	 * 最大空闲连接数
	 */
	private final int maxIdelSize;

	/**
	 * 最大连接数
	 */
	private final int maxSize;
	/**
	 * 连接池实例
	 */
	private static HBaseConnectionPool instance;
	/**
	 * 这个锁也是为了在获取连接的时候，是线程安全的
	 */
	private final Lock lock = new ReentrantLock();
	/**
	 * 等待获取连接的线程
	 */
	private final Condition waitConn = lock.newCondition();
	/**
	 * 是否关闭 强制去堆内存中取值
	 */
	private volatile boolean shutdown = false;

	/**
	 * 连接池的配置对象
	 */
	private final Configuration conf;

	/**
	 * 使用单例设计模式
	 * 
	 * @param initSize
	 *            初始化连接数
	 * @param maxSize
	 *            最大连接数
	 * @param maxIdelSize
	 * @param killTime
	 */
	private HBaseConnectionPool(int initSize, int maxSize, int maxIdelSize, int checkCount, Configuration conf) {
		this.initSize = initSize;
		this.maxSize = maxSize;
		this.maxIdelSize = maxIdelSize;
		this.conf = conf;
		threadPool = Executors.newCachedThreadPool();
		idel = new LinkedBlockingDeque<>();
		active = new LinkedBlockingDeque<>();
		initConnections();
		DetectFailConnection detectThread = new DetectFailConnection(checkCount);
		// 将探测线程设置为守护线程
		detectThread.setDaemon(true);
		detectThread.start();
	}

	/**
	 * 该线程为守护线程，每过30秒查找被关闭的连接 查找失败的连接
	 */
	private class DetectFailConnection extends Thread {
		/**
		 * 探测线程的周期
		 */
		private static final int DetectCycle = 30 * 1000;
		/**
		 * 检查空闲连接的次数
		 */
		private int count;
		/**
		 * 清理空闲连接的次数
		 */
		private final int checkIdelCount;

		public DetectFailConnection(int checkCount) {
			checkIdelCount = checkCount;
		}

		@Override
		public void run() {
			while (true) {
				HBaseUtil.sleep(DetectCycle);
				detectConnection();
				checkKillConnection();
			}
		}

		/**
		 * 周期性检查空闲连接数是否超标 并将其关闭 关闭之后将其移除
		 */
		private void checkKillConnection() {
			if (getIdelSize() > maxIdelSize) {
				count++;
				LOG.debug("count is " + count + " , checkIdelCount is " + checkIdelCount);
				if (count == checkIdelCount) {
					count = 0;
					ConnectionEntity entity = null;
					try {
						entity = idel.takeFirst();
					} catch (InterruptedException e) {
						LOG.error("take fail", e);
					}
					close(entity.connection);
				}
			}
		}

		/**
		 * 周期性的从idel中移除被关闭的连接
		 * 
		 * @param itIdel
		 */
		private void detectConnection() {
			LOG.info("idel size is => " + getIdelSize() + " , active size is => " + getActiveSize());
			checkConn(idel.iterator());
			checkConn(active.iterator());
		}

		private void checkConn(Iterator<ConnectionEntity> iter) {
			while (iter.hasNext()) {
				Connection conn = iter.next().connection;
				if (conn == null || conn.isClosed())
					iter.remove();
			}
		}

	}

	private void initConnections() {
		for (int i = 0; i < initSize; i++) {
			ConnectionEntity entity = createEntity(0);
			idel.add(entity);
		}
	}

	/**
	 * 创建一个未使用的连接实体
	 * 
	 * @return
	 */
	private ConnectionEntity createEntity(int count) {
		Connection conn = createConnection();
		if (conn == null)
			conn = createConnection();
		return new ConnectionEntity(conn, count);
	}

	public static HBaseConnectionPool getInstance() {
		return getInstance(HBaseConfiguration.create());
	}

	/**
	 * 将这个资源文件读取到配置文件中 获取连接池(单例对象)
	 * 
	 * @return
	 */
	public static HBaseConnectionPool getInstance(Configuration conf) {
		Properties pros = HBaseUtil.createProps("hbase");
		for (Entry<Object, Object> entry : pros.entrySet())
			conf.set(entry.getKey().toString(), entry.getValue().toString());
		int initSize = conf.getInt("hbase.init.connection", INIT_CONNSUM);
		int maxSize = conf.getInt("hbase.max.connection", NCPUS);
		int maxIdelSize = conf.getInt("hbase.maxIdel.connection", MAX_IDEL_CONNCUM);
		int count = conf.getInt("hbase.checkIdelConnection.count", KILL_IDEL_CONN_COUNT);
		initSize = Math.max(initSize, 0);
		maxSize = Math.max(maxSize, 1);
		maxIdelSize = Math.max(maxIdelSize, 1);
		count = Math.max(count, KILL_IDEL_CONN_COUNT);
		return getInstance(initSize, maxSize, maxIdelSize, count, conf);
	}

	/**
	 * 根据init和max创建一个连接池
	 * 
	 * @param maxIdelSize
	 * 			最大空闲连接数
	 * @param initSize
	 *            初始连接数
	 * @param maxSize
	 *            最大连接数
	 * @return
	 */
	static HBaseConnectionPool getInstance(int initSize, int maxSize, int maxIdelSize, int count, Configuration conf) {
		if (initSize < 0 || maxSize < 1)
			throw new RuntimeException("initSize < 0，maxsize >= 1");
		if (initSize > maxSize)
			initSize = maxSize;
		// 双重检查
		if (instance == null) {
			synchronized (HBaseConnectionPool.class) {
				if (instance == null)
					instance = new HBaseConnectionPool(initSize, maxSize, maxIdelSize, count, conf);
			}
		}
		return instance;
	}

	/**
	 * 根据配置对象获取一个连接
	 * 
	 * @return
	 */
	private Connection createConnection() {
		int i = 0;
		Connection conn = null;
		// 每次获取连接会重试5次，防止获取失败
		do {
			try {
				conn = ConnectionFactory.createConnection(conf, threadPool);
				if (conn != null)
					break;
				else
					LOG.info("第" + i + "次" + ",获取连接");
				HBaseUtil.sleep(100);
				i++;
			} catch (IOException e) {
				LOG.error("HBase connection error ", e);
			}
		} while (conn == null && i < 5);
		return conn;
	}

	/**
	 * 包装一个连接实体 有其使用次数和连接
	 */
	private class ConnectionEntity {
		/**
		 * 连接的使用次数
		 */
		int usageCount;
		final Connection connection;

		ConnectionEntity(Connection connection, int usageCount) {
			this.usageCount = usageCount;
			this.connection = connection;
		}
	}

	@Override
	public int getIdelSize() {
		return idel.size();
	}

	@Override
	public int getActiveSize() {
		return active.size();
	}

	@Override
	public Connection getConnection() {
		if (isShutdown())
			new RuntimeException("the pool is shutdown!");
		ConnectionEntity entity = null;
		lock.lock();
		try {
			while (getCurrentSize() >= maxSize && getIdelSize() == 0) {
				try {
					LOG.debug("wait to get conn");
					waitConn.await();
				} catch (InterruptedException e) {
					LOG.error("await error!", e);
				}
			}
			if (getIdelSize() > 0) {
				entity = idel.removeFirst();
				entity.usageCount++;
				LOG.debug("this connection usageCount :" + entity.usageCount + " , idel size   :" + getIdelSize()
						+ " , active size :" + getActiveSize() + " , current size:" + getCurrentSize());
			} else {
				entity = createEntity(1);
			}
			active.addLast(entity);
		} finally {
			lock.unlock();
		}
		return entity.connection;
	}

	@Override
	public boolean releaseConnection(Connection connection) {
		if (isShutdown())
			close(connection);
		Iterator<ConnectionEntity> iter = active.iterator();
		ConnectionEntity entity = null;
		while (iter.hasNext()) {
			entity = iter.next();
			if (entity.connection == connection) {
				iter.remove();
				return handlerEntity(entity);
			}
		}
		return close(connection);
	}

	private boolean handlerEntity(ConnectionEntity entity) {
		if (getCurrentSize() > maxSize || entity.usageCount > MAX_USAGE_COUNT)
			return close(entity.connection);
		else {
			idel.addLast(entity);
			lock.lock();
			try {
				waitConn.signal();
			} finally {
				lock.unlock();
			}
		}
		return true;
	}

	@Override
	public void shutdownNow() {
		shutdown = true;
		closeAll();
		threadPool.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return shutdown;
	}

	@Override
	public int getMaxSize() {
		return maxSize;
	}

	@Override
	public synchronized int getCurrentSize() {
		return getActiveSize() + getIdelSize();
	}

	@Override
	public boolean closeConnection(Connection connection) {
		Iterator<ConnectionEntity> iter = active.iterator();
		ConnectionEntity entity = null;
		while (iter.hasNext()) {
			entity = iter.next();
			if (entity.connection == connection) {
				iter.remove();
				return close(entity.connection);
			}
		}
		return close(connection);
	}

	private boolean close(Connection connection) {
		try {
			if (connection != null)
				connection.close();
		} catch (IOException e) {
			LOG.error("close connection error", e);
			return false;
		}
		return true;
	}

	private void closeAll() {
		for (ConnectionEntity entity : idel)
			closeConnection(entity.connection);
		for (ConnectionEntity entity : active)
			closeConnection(entity.connection);
	}

	@Override
	public void shutdown() {
		shutdown = true;
		closeAll();
		threadPool.shutdown();
	}

	public ExecutorService getThreadPool() {
		return threadPool;
	}
}