package com.li.redis.utils;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient {
	public final static String namespace = RedisClient.class.getName();
	public static Map<String, String> _config;

	public static String dg_prefix;
	public static int dg_db_idx;
	public static String gu_prefix;
	public static int gu_db_idx;
	public static String sso_prefix;
	public static int sso_db_idx;
	public static int sso_timeout;
	public static String enteract_prefix;
	public static int enteract_db_idx;
	
	public static String act_prefix;
	public static int act_db_idx;

	
	public static String act_atten_prefix;
	public static int act_atten_idx;
	
	public static JedisPool jedis_pool;

	static {
		reload();
	}

	/**
	 * 初始化非切片池
	 */
	public static void reload() {
		if (null != jedis_pool) {
			jedis_pool.close();
		}
//		_config = Configuration.getConfig(namespace, RedisClient.class);

		JedisPoolConfig jpconf = new JedisPoolConfig();
		jpconf.setMaxIdle(10000);
		jpconf.setMaxWaitMillis(10000L);
		jpconf.setTestOnBorrow(true);
		jpconf.setTestOnReturn(true);
		jedis_pool = new JedisPool(jpconf, "192.168.3",11);

		dg_prefix = _config.get("dept_group.prefix");
		dg_db_idx = Integer.parseInt(_config.get("dept_group.db_idx"));
		gu_prefix = _config.get("group.prefix");
		gu_db_idx = Integer.parseInt(_config.get("group.db_idx"));
		
		enteract_prefix = _config.get("act_enter.prefix");
		enteract_db_idx = Integer.parseInt(_config.get("act.db_idx"));
		
		sso_prefix = _config.get("sso.prefix");
		sso_db_idx = Integer.parseInt(_config.get("sso.db_idx"));
		sso_timeout = Integer.parseInt(_config.get("sso.timeout"));
		
		act_atten_prefix = _config.get("act_atten_prefix");
		act_atten_idx = Integer.parseInt(_config.get("act_atten_idx"));
		
	}
	/**
	 * 
	 * @param actid
	 * @return
	 */
	public static String createEnterActPush(Long actid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(enteract_db_idx);
		String jkey = enteract_prefix + actid;
		String ret = jedis.set(jkey, TimeUtil.getTimeUni());
		jedis.expire(jkey, sso_timeout);
		jedis.close();
		return ret;
	}
	

	public static String getEnterActTime(long actid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(enteract_db_idx);
		String jkey = enteract_prefix + actid;
		String ret = jedis.get(jkey);
		if (null != ret) {
			jedis.expire(jkey, sso_timeout);
		}
		jedis.close();
		return ret;
	}
	
	
	public static long removeEnterActPush(Long actid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(enteract_db_idx);
		long ret = jedis.del(enteract_prefix + actid);
		jedis.close();
		return ret;
	}
	
	
	public static long createAct(long gid, Long... uid) {
		if (null == uid || uid.length < 1) {
			return -1;
		}
		String[] suids = new String[uid.length];
		for (int i = 0; i < uid.length; i++) {
			suids[i] = String.valueOf(uid[i]);
		}
		Jedis jedis = jedis_pool.getResource();
		jedis.select(gu_db_idx);
		long ret = jedis.sadd(gu_prefix + gid, suids);
		jedis.close();
		return ret;
	}
	
	public static String getAct(long actid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_db_idx);
		String jkey = act_prefix + actid;
		String ret =jedis.get(jkey);
		if (null != ret) {
			jedis.expire(jkey, sso_timeout);
		}
		jedis.close();
		return ret;
	}
	

	
	public static long pushActUser(long actid, List<Long> uid) {
		if (null == uid || uid.size() < 1) {
			return -1;
		}
		String[] suids = new String[uid.size()];
		for (int i = 0; i < uid.size(); i++) {
			suids[i] = String.valueOf(uid.get(i));
		}
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_db_idx);
		long ret = jedis.sadd(act_prefix + actid, suids);
		jedis.close();
		return ret;
	}
	public static Set<String> getActUsers(long gid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_db_idx);
		Set<String> rets = jedis.smembers(act_prefix + gid);
		jedis.close();
		return rets;
	}
	public static long createGroup(Long dept_uid, long gid, Long... uid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(dg_db_idx);
		jedis.sadd(dg_prefix + dept_uid, String.valueOf(gid));
		jedis.close();

		return pushGroup(gid, uid);
	}
	
	public static long pushGroup(long gid, Long... uid) {
		if (null == uid || uid.length < 1) {
			return -1;
		}
		String[] suids = new String[uid.length];
		for (int i = 0; i < uid.length; i++) {
			suids[i] = String.valueOf(uid[i]);
		}
		Jedis jedis = jedis_pool.getResource();
		jedis.select(gu_db_idx);
		long ret = jedis.sadd(gu_prefix + gid, suids);
		jedis.close();
		return ret;
	}

	public static long popGroup(long gid, Long... uid) {
		if (null == uid || uid.length < 1) {
			return -1;
		}
		String[] suids = new String[uid.length];
		for (int i = 0; i < uid.length; i++) {
			suids[i] = String.valueOf(uid[i]);
		}
		Jedis jedis = jedis_pool.getResource();
		jedis.select(gu_db_idx);
		long ret = jedis.srem(gu_prefix + gid, suids);
		jedis.close();
		return ret;
	}

	
	public static long popGroupList(List<Long> listgid) {
		long ret=0;
		Jedis jedis = jedis_pool.getResource();
		for(long gid : listgid){
			jedis.select(gu_db_idx);
			ret = jedis.srem(gu_prefix + gid);
		}
		
		jedis.close();
		return ret;
	}
	public static Set<String> getGroupUsers(long gid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(gu_db_idx);
		Set<String> rets = jedis.smembers(gu_prefix + gid);
		jedis.close();
		return rets;
	}

	public static long removeGroup(long gid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(gu_db_idx);
		long ret = jedis.del(gu_prefix + gid);
		jedis.select(dg_db_idx);
		jedis.srem(dg_prefix , String.valueOf(gid));
		jedis.close();
		return ret;
	}

	public static long removeGroupList( List<Long> listgid) {
		long ret=0;
			Jedis jedis = jedis_pool.getResource();
			for(long gid : listgid){
				jedis.select(gu_db_idx);
				ret = jedis.del(gu_prefix + gid);
				jedis.select(dg_db_idx);
				jedis.srem(dg_prefix , String.valueOf(gid));
			}
			jedis.close();
		return ret;
		
	}
	/**
	 * 返回的是uid
	 * 
	 * @param session
	 * @return
	 */
	public static String getSession(String uid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(sso_db_idx);
		String jkey = sso_prefix + uid;
		String ret = jedis.get(jkey);
		if (null != ret) {
			jedis.expire(jkey, sso_timeout);
		}
		jedis.close();
		return ret;
	}

	public static String setSession(String uid, String session) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(sso_db_idx);
		String jkey = sso_prefix + uid;
		String ret = jedis.set(jkey, session);
		jedis.expire(jkey, sso_timeout);
		jedis.close();
		return ret;
	}

	public static long removeSession(String session) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(sso_db_idx);
		long ret = jedis.del(sso_prefix + session);
		jedis.close();
		return ret;
	}
	
	public static long pushActAttenUser(long actid, List<Long>  uid) {
		if (null == uid || uid.size() < 1) {
			return -1;
		}
		String[] suids = new String[uid.size()];
		for (int i = 0; i < uid.size(); i++) {
			suids[i] = String.valueOf(uid.get(i));
		}
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_atten_idx);
		long ret = jedis.sadd(act_atten_prefix + actid, suids);
		jedis.close();
		return ret;
	}
	
	public static long removeActAttenUser(long actid) {
		long ret=0;
		Jedis jedis = jedis_pool.getResource();
			jedis.select(act_atten_idx);
			ret = jedis.srem(act_atten_prefix + actid);
		jedis.close();
		return ret;
	}
	public static List<Long> getActAttenUser(long actid) {
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_atten_idx);
		Set<String> rets = jedis.smembers(act_atten_prefix + actid);
		jedis.close();
		List<Long> list=new ArrayList<Long>();
		for(String uid : rets){
			list.add(Long.parseLong(uid));
		}
		return list;
	}
	public static long popActAttenUser(long actid, Long uid) {
		String[] suids = new String[1];
		suids[0] = String.valueOf(uid);
		Jedis jedis = jedis_pool.getResource();
		jedis.select(act_atten_idx);
		long ret = jedis.srem(act_atten_prefix + actid, suids);
		jedis.close();
		return ret;
	}
	
//	
//	public static long pushOwnerIdActAttenUser(long ownerId, List<Long>  uid) {
//		if (null == uid || uid.size() < 1) {
//			return -1;
//		}
//		String[] suids = new String[uid.size()];
//		for (int i = 0; i < uid.size(); i++) {
//			suids[i] = String.valueOf(uid.get(i));
//		}
//		Jedis jedis = jedis_pool.getResource();
//		jedis.select(7);
//		long ret = jedis.sadd("ownerid:" + ownerId, suids);
//		jedis.close();
//		return ret;
//	}
//	
//	public static long removeOwnerIdActAttenUser(long ownerId) {
//		long ret=0;
//		Jedis jedis = jedis_pool.getResource();
//			jedis.select(7);
//			ret = jedis.srem("ownerid:" + ownerId);
//		jedis.close();
//		return ret;
//	}
//	public static List<Long> getOwnerIdActAttenUser(long ownerId) {
//		Jedis jedis = jedis_pool.getResource();
//		jedis.select(7);
//		Set<String> rets = jedis.smembers("ownerid:" + ownerId);
//		jedis.close();
//		List<Long> list=new ArrayList<Long>();
//		for(String uid : rets){
//			list.add(Long.parseLong(uid));
//		}
//		return list;
//	}
//	public static long popOwnerIdActAttenUser(long ownerId, Long uid) {
//		String[] suids = new String[1];
//		suids[0] = String.valueOf(uid);
//		Jedis jedis = jedis_pool.getResource();
//		jedis.select(7);
//		long ret = jedis.srem("ownerid:" + ownerId, suids);
//		jedis.close();
//		return ret;
//	}
//	public static void main(String[] args) {
//			new RedisClient().removeEnterActPush(760L);
//	}
}
