package com.wang.hbase;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Connection;

public interface IConnectionPool {
	/**
	 * 连接池是否关闭
	 * 
	 * @return
	 */
	boolean isShutdown();

	/**
	 * 
	 * @return 空闲连接数
	 */
	int getIdelSize();

	/**
	 * @return 被使用的连接数
	 */
	int getActiveSize();

	/**
	 * 最大连接数
	 * 
	 * @return
	 */
	int getMaxSize();

	/**
	 * 当前连接数
	 * 
	 * @return
	 */
	int getCurrentSize();

	/**
	 * 从连接池中获取一个连接
	 * 
	 * @return
	 * @throws InterruptedException 
	 */
	Connection getConnection();

	/**
	 * 释放一个连接
	 * 
	 * @param connection
	 * @return 是否释放成功
	 */
	boolean releaseConnection(Connection connection);

	/**
	 * 关闭连接
	 * 
	 * @param connection
	 *            连接对象
	 * @return 是否关闭成功
	 */
	boolean closeConnection(Connection connection);

	/**
	 * 关闭连接池
	 */
	void shutdown();

	/**
	 * 立即关闭连接池
	 */
	void shutdownNow();
	
	/**
	 * @return
	 * 获取批量操作的线程池
	 */
	ExecutorService getThreadPool();

}
