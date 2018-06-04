package com.etc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;

/**
 * 鏁版嵁搴撴搷浣滅殑杈呭姪绫�
 */
public class DBUtil {
	private static final String DRIVER = "oracle.jdbc.OracleDriver";
	private static final String URL = "jdbc:oracle:thin:@localhost:1521:orcl";

	private static final String USER = "scott"; // 用户名
	private static final String PASSWORD = "tiger";// 密码
	
	private static final String MSG = "tiger";// MSG


	private static final String pwd = "tiger";// 瀵嗙爜


	/**
	 * 鑾峰彇杩炴帴瀵硅薄
	 * 
	 * @return 杩炴帴瀵硅薄
	 */
	public static Connection getConn() {

		Connection conn = null;
		try {

			Class.forName(DRIVER);
			// 寰楀埌杩炴帴瀵硅薄
			conn = DriverManager.getConnection(URL, USER, PASSWORD);

		} catch (Exception e) {
			throw new RuntimeException("鏁版嵁搴撹繛鎺ュけ璐�!", e);
		}
		return conn;
	}

	/**
	 * 閲婃斁璧勬簮
	 * 
	 * @param rs
	 *            缁撴灉闆�
	 * @param pstmt
	 *            鍛戒护澶勭悊瀵硅薄
	 * @param conn
	 *            杩炴帴瀵硅薄
	 */
	public static void close(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException("閲婃斁璧勬簮澶辫触!", e);
		}
	}

	/**
	 * 璁剧疆鍙傛暟
	 * 
	 * @param sql
	 * @param conn
	 * @param pstmt
	 * @param param
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement setPstmt(String sql, Connection conn, PreparedStatement pstmt, Object... param)
			throws SQLException {
		pstmt = conn.prepareStatement(sql);
		if (param != null) {
			for (int i = 0; i < param.length; i++) {
				pstmt.setObject(i + 1, param[i]);
			}
		}
		return pstmt;
	}

	/**
	 * 閫氱敤鐨勬暟鎹簱(澧�,鍒�,鏀�)鎿嶄綔鏂规硶
	 * 
	 * @param sql
	 *            sql璇彞
	 * @param param
	 *            sql璇彞鍙傛暟
	 * @return 鍙楀奖鍝嶈鏁�
	 */
	public static int execute(String sql, Object... param) {
		Connection conn = getConn();
		try {
			return execute(sql, conn, param);
		} finally {
			close(null, null, conn);
		}
	}

	/**
	 * 閫氱敤鐨勫鍒犳敼鎿嶄綔(浜嬪姟璁块棶)
	 * 
	 * @param sql
	 * @param conn
	 * @param param
	 * @return
	 */
	public static int execute(String sql, Connection conn, Object... param) {
		PreparedStatement pstmt = null;
		try {
			pstmt = setPstmt(sql, conn, pstmt, param);
			return pstmt.executeUpdate();
		} catch (SQLException e) {
			// 杩欓噷鏈�濂藉緱鍒板紓甯镐俊鎭�
			e.printStackTrace();
			throw new RuntimeException("鏁版嵁搴撴搷浣滃け璐�!", e);
		} finally {
			close(null, pstmt, null);
		}
	}

	/**
	 * 閫氱敤鏌ヨ鏂规硶
	 * 
	 * @param sql
	 *            瑕佹煡璇㈢殑sql璇彞
	 * @param cla
	 *            Class瀵硅薄
	 * @param param
	 *            鍙傛暟
	 * @return
	 */
	public static Object select(String sql, Class cla, Object... param) {
		Connection conn = getConn();
		try {
			return select(sql, conn, cla, param);
		} finally {
			close(null, null, conn);
		}
	}

	/**
	 * 甯︿簨鍔＄殑鏌ヨ鏂规硶
	 * 
	 * @param sql
	 * @param conn
	 * @param cla
	 * @param param
	 * @return
	 */
	public static Object select(String sql, Connection conn, Class cla, Object... param) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		List<Object> list = new ArrayList<Object>();
		try {
			pstmt = setPstmt(sql, conn, pstmt, param);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				// ?rs 缁撴灉闆� cla Class瀵硅薄
				// object鍏跺疄灏辨槸鏁版嵁琛ㄧ粨鏋勫搴旂殑涓�鏉″疄浣撹褰�,object灏辨槸閭ｄ釜瀹炰綋绫诲璞�
				// 杩欎釜鏂规硶convert鏄皢缁撴灉闆嗗拰cla瀵硅薄杩涜杞崲
				Object object = convert(rs, cla);
				list.add(object);
			}
			return list;
		} catch (SQLException e) {
			throw new RuntimeException("鏁版嵁搴撴煡璇㈠け璐�!", e);
		} finally {
			close(rs, pstmt, null);
		}
	}

	/**
	 * 鑾峰彇鍗曚釜璁板綍鍊�,鏄崟涓褰曟敞鎰�,绫讳技count(1)
	 * 
	 * @param sql
	 * @param param
	 * @return
	 */
	public static Object getFirst(String sql, Object... param) {
		Connection conn = getConn();
		try {
			return getFirst(sql, conn, param);
		} finally {
			close(null, null, conn);
		}
	}

	/**
	 * 鑾峰彇鍗曚釜璁板綍 浜嬪姟
	 * 
	 * @param sql
	 * @param conn
	 * @param param
	 * @return
	 */
	public static Object getFirst(String sql, Connection conn, Object... param) {
		List list = (List) select(sql, conn, Object.class, param);
		if (list.isEmpty()) {
			return null;
		}
		return list.get(0);
	}

	/**
	 * 浜嬪姟澶勭悊鎿嶄綔
	 * 
	 * @param tran
	 * @return
	 */
	public static Object transaction(ITransaction tran) {
		Connection conn = getConn();
		try {
			conn.setAutoCommit(false);
			Object obj = tran.execute(conn);
			conn.commit();
			return obj;
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new RuntimeException("鍥炴粴澶辫触!", e);
			}
			throw new RuntimeException("浜嬪姟鎵ц澶辫触", e);
		} finally {
			close(null, null, conn);
		}
	}

	/**
	 * 鏌ヨ缁撴灉鐨勮浆鎹�(鍏崇郴鏁版嵁搴� <=>java涓被闈㈠悜瀵硅薄)
	 * 
	 * @param rs
	 *            缁撴灉闆嗗悎
	 * @param cla
	 *            Class绫诲璞�
	 * @return
	 */
	public static Object convert(ResultSet rs, Class cla) {
		try {
			// getName 绫诲悕 鍖呭惈浜嗗畬鏁村寘缁撴瀯绫诲悕
			if (cla.getName().equals("java.lang.Object")) {
				return rs.getObject(1);
			}
			// 鍒涘缓瀹炰綋绫荤殑瀹炰緥 Class绫诲璞＄殑鏂规硶锛屽垱寤烘寚瀹氬璞＄殑瀹炰緥
			// new Goods(); new News(); new person(); new Users();
			// newInstance 浼氳皟鐢ㄥ疄浣撶被鐨� 鏃犲弬鏁扮殑鏋勯��
			Object object = cla.newInstance();
			//// 缁撴灉闆嗗ご淇℃伅瀵硅薄
			// rs.getMetaData() 鑾峰彇姝� ResultSet 瀵硅薄鐨勫垪鐨勭紪鍙枫�佺被鍨嬪拰灞炴�с��
			///
			ResultSetMetaData metaData = rs.getMetaData();
			// 寰幆涓哄疄浣撶被鐨勫疄渚嬬殑灞炴�ц祴鍊� getColumnCount寰楀埌鍒楃殑涓暟
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				// 鑾峰彇鍒楀悕 name
				String name = metaData.getColumnLabel(i);
				//// 鎵�鏈夎娉ㄦ剰锛氬垪鍚峓鏌ヨ璇彞涓垪鍚峕涓庡睘鎬у悕蹇呴』涓�鑷淬�傛渶濂介伒寰獑椹煎懡鍚嶆柟娉�. rs.getObject(i) 缁撴灉闆嗕腑鐨勬煡璇㈢粨鏋滃拰瀵硅薄鍖归厤
				// select empNo as eNo,empName as eName from employee
				BeanUtils.setProperty(object, name, rs.getObject(i));
			}
			return object;
		} catch (Exception e) {
			throw new RuntimeException("灞炴�ц缃け璐�!", e);
		}
	}

	/**
	 * 鍒嗛〉鎿嶄綔 mysql;
	 * 
	 * @param sql
	 *            鏌ヨ鐨勮鍙�
	 * @param page
	 *            椤电爜(绗嚑椤�)
	 * @param pageSize
	 *            姣忛〉鐨勮褰曟暟()
	 * @param cla
	 *            鏌ヨ鐨勬槸鍝釜琛�->瀹炰綋绫�(Users.class Aritcle.class)
	 * @param param
	 *            鏌ヨ鐨勬潯浠跺搴旂殑鍙傛暟绛�
	 * @return PageData(椤甸潰鏁版嵁)
	 */
	public static PageData getPage(String sql, Integer page, Integer pageSize, Class cla, Object... param) {
		// 寰楀埌璁板綍鏁� t灏辨槸涓�涓埆鍚� (" + sql + ") 鏌ヨ缁撴灉 ,灏嗚繖涓粨鏋滆捣涓悕瀛� t
		String selSql = "select count(1) from (" + sql + ") t";
		if (page == null) {
			page = 1;
		}
		if (pageSize == null) {
			pageSize = 10;
		}
		// 寰楀埌浜嗘�昏褰曟暟鏈夊嚑鏉� count
		Integer count = Integer.parseInt(getFirst(selSql, param).toString());
		// page =>1 0*10 =>start =>0 page =>2 1*10 =>start =>10
		// 璧峰浣嶇疆鐨勫��
		int start = (page - 1) * pageSize;
		// SELECT * from users limit 3,3 -- 鏄剧ず鐨勮褰曟槸浠巙serId 4寮�濮�
		selSql = sql + " limit " + start + "," + pageSize;
		List list = (List) select(selSql, cla, param);
		// 鍒涘缓浜嗕竴涓� PageData
		PageData data = new PageData(list, count, pageSize, page);
		return data;
	}

	/**
	 * 鍒嗛〉鎿嶄綔 sqlserver
	 * 
	 * @param page
	 * @param pageSize
	 * @param cla
	 * @param identity
	 * @return
	 */
	public static PageData getPage(Integer page, Integer pageSize, Class cla, String identity) {
		String name = cla.getName().substring(cla.getName().lastIndexOf(".") + 1);// 鏍规嵁鍛藉悕瑙勫垯浠庣被鍚嶈幏鍙栨暟鎹簱琛ㄥ悕
		String selSql = "select count(*) from " + name;// 鑾峰彇鎬绘暟
		if (page == null) {
			page = 1;
		}
		if (pageSize == null) {
			pageSize = 20;
		}
		int start = (page - 1) * pageSize;
		Integer count = Integer.parseInt(getFirst(selSql, null).toString());
		selSql = "select top " + pageSize + " * from " + name + " where " + identity + " not in (select top " + start
				+ " " + identity + " from " + name + " )"; // 鎷兼帴鏌ヨ璇彞
		List list = (List) select(selSql, cla, null);
		PageData data = new PageData(list, count, pageSize, page);
		return data;
	}

	/**
	 * oracle鐨勫垎椤靛疄鐜�
	 * 
	 * @param sql  鎵ц鐨剆ql璇彞  (select * from emp where ename like ?)
	 * @param page  褰撳墠椤电爜
	 * @param pageSize  姣忛〉鏄剧ず鐨勮褰曟暟
	 * @param cla  Class绫诲璞�(Emp.class Dept.class)
	 * @param param  sql璇彞涓殑? ("%S%")
	 * @return   鏄竴涓狿ageData瀵硅薄
	 */
	public static PageData getOraclePage(String sql, Integer page, Integer pageSize, Class cla, Object... param) {
		// sql select * from news
		// select count(1) from (select * from news) t --寰楀埌璁板綍鎬绘暟
		String selSql = "select count(1) from (" + sql + ") t";
		if (page == null) {
			page = 1;
		}
		if (pageSize == null) {
			pageSize = 10;
		}
		// 鏌ヨ寰楀埌鎬昏褰曟暟
		Integer count = Integer.parseInt(getFirst(selSql, param).toString());
		// 瀹炵幇绠�鍗曠殑鍒嗛〉璇彞
		// 濡傛灉鏄痮racle鏁版嵁搴撶殑璇� 搴旇浣跨敤rownum鏉ュ疄鐜扮畝鍗曠殑鍒嗛〉鎿嶄綔
		int start = (page - 1) * pageSize; // 璧峰浣嶇疆绠楁硶
		// + 鍏跺疄涓嶅お濂� 鏈�濂界敤stringBuffer stringBuilder append

		// rownum<=10 杩欎釜10搴旇鏄粨鏉熶綅缃� page = 2 姣忛〉鏄剧ず10鏉� ->绗�11鍒扮20鏉�
		int end = page * pageSize;
		// r>5 杩欎釜鏄繃婊ゆ帀鐨勯儴鍒�

		// sql 鍏跺疄灏辨槸 (select * from emp) tbl
		String oracleSql = "select * from (select tbl.*,rownum r from (" + sql + ") tbl where rownum<=" + end
				+ ") mytable  where r>" + start;

		List list = (List) select(oracleSql, cla, param);
		// 鍒涘缓涓�涓狿ageData瀵硅薄
		PageData data = new PageData(list, count, pageSize, page);
		return data;
	}

}
