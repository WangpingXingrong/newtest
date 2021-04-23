/************************************************************************
* システム名      ：  情報配信サービス
* 導入先システムバージョン  ：  無し
* ファイルバージョン：  0100
* JavaAPI      ：  OpenJDK 11.0.4.11
* 作成日       ：  2019.10.18
* 作成者       ：  BHH
* 作成内容     ：  設定値取得クラス
* 変更履歴     ：
* DatabaseHandleClassクラスを新規作成
* 2019/11/07 (BHH)fucq CT不良対応 00-01-00-00(CT#00010)
* Copyright(c)    ：Hitachi Kokusai Electric Inc. 2019 All Rights Reserved．
***********************************************************************/

package hike.OuterConnectionSystem.CommonLibrary;

import hike.OuterConnectionSystem.CommonLibrary.CommonClass.FaxInfoEntity;
import hike.OuterConnectionSystem.CommonLibrary.CommonClass.LineAccountInfo;
import hike.OuterConnectionSystem.CommonLibrary.CommonClass.MailAddress;
import hike.OuterConnectionSystem.CommonLibrary.CommonClass.TwitterAccountInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * 設定値取得.
 * <pre>
 * FileName: DatabaseHandleClass.java
 * </pre>
 * @author (BHH)
 */
public class DatabaseHandleClass {
  //ログ処理
  private static Logger logger = Logger.getLogger(DatabaseHandleClass.class);

  /**
   * SYSTEM_SETTINGテーブルを読み取る. 
   * 
   * @param setid    設定値ID
   * @return 設定値
   */
  public static String systemSettingQuery(int setid) {

    String setvalue = null;
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = null;
    ResultSet re = null;
    // SQL文取得
    sql = "SELECT set_id, set_value FROM system_setting";

    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        if (re.getString(1).equals(String.valueOf(setid))) {
          setvalue = re.getString(2);
        }
      }
      // 取得した規定値がnullの場合、settingConfig.jsonから取得
      if (setvalue == null || setvalue.isEmpty()) {
        jdbc.release(con, statement, re);
        return null;
      }
    } catch (SQLException e) {
      logger.warn(e);
      // DBから取得異常の場合、settingConfig.jsonから取得
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return setvalue;
  }
  
  /**
   * 区名リストを読み取る.
   * <pre>
   * 下記配信先用：
   * ・登録制メール接続
   * ・防災情報システム
   * ・職員参集メール
   *  
   * ②配信先に市番号が含まれる場合は、他の区番号、区+町番号、区＋町＋丁番号指定を無視して、区名「各区」のみを追加
   * 配信先に「各区役所」番号が含まれる場合は、他の区役所番号指定を無視し、区役所名「各区役所」のみを追加
   * 配信先に「全室局」番号が含まれる場合は、他の室局番号指定を無視し、室局名「全室局」のみを追加
   * (②以外の場合、下記にて区名に変換)
   * ③区番号指定の場合(＝町、丁番号がともに0の場合)
   * 当該区名を追加
   * ④区+町番号指定の場合(＝丁番号が0の場合)
   * 当該区名を追加
   * ⑤区＋町＋丁番号指定の場合
   * 当該区名を追加
   * ⑥重複区名を除外
   * 
   * 対象無しの場合、リストのcount=0
   * DB異常場合、nullを戻る
   * </pre>
   * 
   * @param districtIdLst 区IDリスト
   * @param enableLst 利用対象リスト（区、区役所、室局）
   * @return 対象区名リスト
   */
  public static ArrayList<String> getDistrictNameList(ArrayList<Long> districtIdLst, 
      ArrayList<Long> enableLst) {
    ArrayList<String> districtNmLst = new ArrayList<String>();

    if (enableLst.size() == 0 || districtIdLst.size() == 0) {
      //対象無し
      return districtNmLst;
    }
    
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    ResultSet re = null;
    try {
      statement = con.createStatement();
      if (districtIdLst.contains(CommonClass.allCityId) && enableLst.contains(Const.DISTRICT)) {
        // 配信先に市番号が含まれる
        String sql = "SELECT a.area_name "
            + "FROM district d "
            + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
            + "WHERE district_type = " + Const.DISTRICT 
            + " AND district_id = " + CommonClass.allCityId;
        // SQL文実行
        re = statement.executeQuery(sql);
        if (re.next()) {
          String tmp = re.getString(1);
          if (tmp != null) {
            districtNmLst.add(tmp);
            //「各区」のみを追加、別の区情報無視
            enableLst.remove(Const.DISTRICT);
          }
        }
      }
      if (districtIdLst.contains(CommonClass.allHallId) && enableLst.contains(Const.HALL)) {
        // 配信先に「各区役所」番号が含まれる
        String sql = "SELECT a.area_name "
            + "FROM district d "
            + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
            + "WHERE district_type = " + Const.HALL 
            + " AND district_id = " + CommonClass.allHallId;
        // SQL文実行
        re = statement.executeQuery(sql);
        if (re.next()) {
          String tmp = re.getString(1);
          if (tmp != null) {
            districtNmLst.add(tmp);
            //「各区役所」のみを追加、別の区役所情報無視
            enableLst.remove(Const.HALL);
          }
        }
      }
      if (districtIdLst.contains(CommonClass.allOfficeId) && enableLst.contains(Const.OFFICE)) {
        // 配信先に「全室局」番号が含まれる
        String sql = "SELECT a.area_name "
            + "FROM district d "
            + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
            + "WHERE district_type = " + Const.OFFICE 
            + " AND district_id = " + CommonClass.allOfficeId;
        // SQL文実行
        re = statement.executeQuery(sql);
        if (re.next()) {
          String tmp = re.getString(1);
          if (tmp != null) {
            districtNmLst.add(tmp);
            //「全室局」のみを追加、別の室局情報無視
            enableLst.remove(Const.OFFICE);
          }
        }
      }
      if (enableLst.size() == 0) {
        //対象無し、再検索不要
        jdbc.release(con, statement, re);
        return districtNmLst;
      }
      String enablelist = enableLst.toString();
      String disIdlist = districtIdLst.toString();
      String sql = "SELECT a.area_name "
          + "FROM district d "
          + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
          + "WHERE district_type IN (" + enablelist.substring(1, enablelist.length() - 1) + ") "
              + "AND district_id IN (" + disIdlist.substring(1, disIdlist.length() - 1) + ")";
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        String tmp = re.getString(1);
        if (tmp != null && !districtNmLst.contains(tmp)) {
          districtNmLst.add(tmp);
        }
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return districtNmLst;
  }
  
  /**
   * 利用対象かどうか判定. 
   * 
   * @param districtId 区ID
   * @param enableLst 利用対象リスト（区、区役所、室局）
   * @return 利用対象場合1、無視対象場合0、DB異常場合-1
   */
  public static int isMine(long districtId, ArrayList<Long> enableLst) {
    // 全区、各区役所、全室局判定
    if ((districtId == CommonClass.allCityId && enableLst.contains(Const.DISTRICT)) 
        || (districtId == CommonClass.allHallId && enableLst.contains(Const.HALL))
        || (districtId == CommonClass.allOfficeId && enableLst.contains(Const.OFFICE))) {
      return Const.YES;
    }

    if (enableLst.size() == 0) {
      //対象無し
      return Const.NO;
    }
    
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return Const.ERROR;
    }
    // DB接続成功
    Statement statement = null;
    String enablelist = enableLst.toString();
    String sql = "SELECT district_id FROM district WHERE district_id = " + districtId 
        + " AND district_type IN (" + enablelist.substring(1, enablelist.length() - 1) + ")";
    ResultSet re = null;
    int result = Const.NO;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result = Const.YES;
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      result = Const.ERROR;
    }
    jdbc.release(con, statement, re);
    
    return result;
  }
    
  /**
   * 区名を読み取る. 
   * <pre>
   * 下記配信先用：
   * ・CATVテロップ(J:COM)
   * ・CATVテロップ(Baycom)
   * 
   * 対象区テーブルを参照し、CATVテロップの対象区が該当する区名リストを戻る。
   * 重複区名を除外
   * 対象無しの場合、リストのcount=0
   * </pre>
   * 
   * @param districtIdLst 区IDリスト
   * @param catvCode 会社コード
   * @return 対象区名リスト
   * @version
   * <pre>
   * 2019/11/07 (BHH)fucq CT不良対応 00-01-00-00(CT#00010)
   * </pre>
   */
  public static ArrayList<String> getDistrictNameListCatv(ArrayList<Long> districtIdLst,
      long catvCode) {
    ArrayList<String> districtNmLst = new ArrayList<String>();
    if (districtIdLst.size() == 0 
        || (catvCode != Const.JCOM && catvCode != Const.BAYCOM)) {
      //対象無し
      return districtNmLst;
    }

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "";
    if (districtIdLst.contains(CommonClass.allCityId)) {
      // 全市場合、全体対象区番号を戻る
      sql = "SELECT a.area_name "
          + "FROM catv_district c "
          + "LEFT OUTER JOIN district d ON c.district_id = d.district_id "
          + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
          + "WHERE d.district_type = 1 "
          + "AND a.lang_type = 0 "
          + "AND catv_cd = " + catvCode;
    } else {
      String disIdlist = districtIdLst.toString();
      sql = "SELECT a.area_name "
          + "FROM catv_district c "
          + "LEFT OUTER JOIN district d ON c.district_id = d.district_id "
          + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
          + "WHERE c.district_id IN (" + disIdlist.substring(1, disIdlist.length() - 1) + ") "
          + "AND d.district_type = 1 "
          + "AND a.lang_type = 0 "
          + "AND catv_cd = " + catvCode;
    }
    
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        String tmp = re.getString(1);
        if (tmp != null && !districtNmLst.contains(tmp)) {
          districtNmLst.add(tmp);
        }
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return districtNmLst;
  }
  
  /**
   * 入力したSQL文基にLongリスト取得. 
   * 
   * @param sql SQL文
   * @return Longリスト
   */
  public static ArrayList<Long> getLongListwithSql(String sql) {
    ArrayList<Long> stationId = new ArrayList<Long>();
    if (sql == null || sql.isEmpty()) {
      // 結果無し
      return stationId;
    }

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        stationId.add(re.getLong(1));
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return stationId;
  }
  
  /**
   * 大阪市全体区ID取得. 
   * @return 区IDリスト
   */
  public static ArrayList<Long> getAllDistrictId() {
    
    ArrayList<Long> stationId = new ArrayList<Long>();
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    ResultSet re = null;
    String sql = "SELECT district_id FROM district WHERE district_type = " + Const.DISTRICT;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        stationId.add(re.getLong(1));
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return stationId;
  }
  
  /**
   * 同報システム配信用、区＋町名前を読み取る. 
   * <pre>
   * 区ID=0場合、全市の区名+町名
   * 町ID=0場合、区名+全町名
   * </pre>
   * @param districtId 区ID
   * @param townId 町ID
   * @return 区＋町名
   */
  public static ArrayList<String> getDistrictTown(long districtId, long townId) {
    ArrayList<String> districtTownName = new ArrayList<String>();

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "";
    if (districtId == CommonClass.allCityId) {
      //全市
      sql = "SELECT a.area_name, d.district_id "
          + "FROM district d "
          + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
          + "WHERE d.district_id <> " + CommonClass.allCityId 
          + " AND d.district_type = " + Const.DISTRICT;
      ResultSet re = null;
      Statement statement1 = null;
      try {
        statement = con.createStatement();
        // SQL文実行
        re = statement.executeQuery(sql);
        while (re.next()) {
          String districtName = re.getString(1);
          long disId = re.getLong(2);
          if (districtName != null) {
            // 区名有り、町名を取得
            if (townId == CommonClass.allDistrictId) {
              //全区
              sql = "SELECT a.area_name "
                  + "FROM town t "
                  + "LEFT OUTER JOIN area_name a ON t.area_name_cd = a.area_name_cd "
                  + "WHERE t.district_id = " + disId;
            } else {
              sql = "SELECT a.area_name "
                  + "FROM town t "
                  + "LEFT OUTER JOIN area_name a ON t.area_name_cd = a.area_name_cd "
                  + "WHERE t.district_id = " + disId 
                  + " AND t.town_id = " + townId;
            }
            statement1 = con.createStatement();
            ResultSet result = statement1.executeQuery(sql);
            while (result.next()) {
              String tmp = result.getString(1);
              if (tmp != null) {
                String districtTown = districtName + tmp;
                if (!districtTownName.contains(districtTown)) {
                  districtTownName.add(districtTown);
                }
              }
            }
            statement1.close();
          }
        } 
      } catch (SQLException e) {
        logger.warn("SQL run error. " + e);
        // DBから取得異常
        if (statement1 != null) {
          try {
            statement1.close();
          } catch (SQLException e1) {
            logger.warn(e1);
          }
        }
        jdbc.release(con, statement, re);
        return null;
      }
      jdbc.release(con, statement, re);      

      return districtTownName;
    }

    //本区
    sql = "SELECT a.area_name "
        + "FROM district d "
        + "LEFT OUTER JOIN area_name a ON d.area_name_cd = a.area_name_cd "
        + "WHERE d.district_id = " + districtId 
        + " AND d.district_type = " + Const.DISTRICT;
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        String districtName = re.getString(1);
        if (districtName != null) {
          // 区名有り、町名を取得
          if (townId == CommonClass.allDistrictId) {
            //全区
            sql = "SELECT a.area_name "
                + "FROM town t "
                + "LEFT OUTER JOIN area_name a ON t.area_name_cd = a.area_name_cd "
                + "WHERE t.district_id = " + districtId;
          } else {
            sql = "SELECT a.area_name "
                + "FROM town t "
                + "LEFT OUTER JOIN area_name a ON t.area_name_cd = a.area_name_cd "
                + "WHERE t.district_id = " + districtId 
                + " AND t.town_id = " + townId;
          }
          re = statement.executeQuery(sql);
          while (re.next()) {
            String tmp = re.getString(1);
            if (tmp != null) {
              String districtTown = districtName + tmp;
              if (!districtTownName.contains(districtTown)) {
                districtTownName.add(districtTown);
              }
            }
          }
        } else {
          // 区名空
          jdbc.release(con, statement, re);
          return districtTownName;
        }
      } else {
        // 区名無し
        jdbc.release(con, statement, re);
        return districtTownName;
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
  
    return districtTownName;
  }
  
  /**
   * 装置番号を読み取る. 
   * @param connectId 連携先ID
   * @return 装置番号 取得異常場合 -1
   */
  public static long getSystemId(long connectId) {
    
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return Const.ERROR;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT device_id FROM connect_system "
        + "WHERE system_id = " + connectId;
    ResultSet re = null;
    long result = Const.ERROR;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result = re.getLong(1);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      result = Const.ERROR;
    }
    jdbc.release(con, statement, re);
    
    return result;
  }
  
  /**
   * 装置名称を読み取る. 
   * @param connectId 連携先ID
   * @return 装置名称 取得異常場合 null
   */
  public static String getSystemName(long connectId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT system_name FROM connect_system "
        + "WHERE system_id = " + connectId;
    ResultSet re = null;
    String result = "";
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result = re.getString(1);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    
    return result;
  }
  
  
  /**
   * 24区ID取得. 
   * @param companyCode 会社コード
   * @return 24区IDリスト
   */
  public static ArrayList<Long> getDistrictAll(long companyCode) {
    ArrayList<Long> districtAll = new ArrayList<Long>();
    if (companyCode != Const.NTTDOCOMO 
        && companyCode != Const.KDDI
        && companyCode != Const.SOFTBANK
        && companyCode != Const.LOTTE
        && companyCode != Const.YAHOO) {
      //対象無し
      return districtAll;
    }

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT district_id FROM deliv_area "
        + "WHERE company_cd = " + companyCode;
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        districtAll.add(re.getLong(1));
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return districtAll;
  }
  
  /**
   * 配信エリアコードを読み取る.
   * <pre>
   * 下記配信先用：
   * ・NTTDocomo
   * ・KDDI
   * ・SoftBank
   * ・楽天
   * ・Yahoo緊急速報
   *  
   * 区ID(対象ID)を基に、当該会社の配信エリアコードリストを読み取る。
   * </pre>
   * 
   * @param districtIdLst 区IDリスト
   * @param companyCode 会社コード
   * @return 配信エリアコードリスト
   */
  public static ArrayList<String> getAreaCodeList(ArrayList<Long> districtIdLst,long companyCode) {
    ArrayList<String> districtNmLst = new ArrayList<String>();   
    if (districtIdLst.size() == 0 
        || (companyCode != Const.NTTDOCOMO
          && companyCode != Const.KDDI
          && companyCode != Const.SOFTBANK
          && companyCode != Const.LOTTE
          && companyCode != Const.YAHOO)) {
      //対象無し
      return districtNmLst;
    }
    
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String disIdlist = districtIdLst.toString();
    String sql = "SELECT deliv_area_cd FROM deliv_area "
        + "WHERE company_cd = " + companyCode 
        + " AND district_id IN (" + disIdlist.substring(1, disIdlist.length() - 1) + ")";
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        districtNmLst.add(re.getString(1));
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return districtNmLst;
  }
  
  /**
   * Enum区分名称取得. 
   * @param groupId 区分グループId
   * @param codeId 区分値Id
   * @return 区分名称
   */
  public static String getCodeName(long groupId, int codeId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = String.format("SELECT code_name FROM code_setting "
        + "WHERE group_id=%d AND code_id=%d", groupId, codeId);
    ResultSet re = null;
    String result = "";
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result = re.getString(1);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return result;
  }
  
  /**
   * 要請理由取得. 
   * @param categoryId 要請理由Enum値
   * @return 要請理由
   */
  public static String getCategory(int categoryId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = String.format("SELECT category_name FROM category "
        + "WHERE category_id = " +  categoryId);
    ResultSet re = null;
    String result = "";
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result = re.getString(1);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return result;
  }
  
  /**
   * メールアドレス取得. 
   * @param mailId メールID
   * @return メールアドレスObject
   */
  public static MailAddress getMailAddress(long mailId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT * FROM mail_addr "
        + "WHERE mail_addr_id = " + mailId;
    ResultSet re = null;
    MailAddress result = new MailAddress();
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result.mailAddr = re.getString(2);
        result.recvIpAddr = re.getString(3);
        result.recvPortNum = re.getInt(4);
        result.recvEncTyp = re.getInt(5);
        result.recvPortTyp = re.getInt(6);
        result.recvUserID = re.getString(7);
        result.recvPasswd = re.getString(8);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return result;
  }
  
  /**
   * LINEアカウント取得. 
   * @param lineId LineID
   * @return LINEアカウントObject
   */
  public static LineAccountInfo getLineAccountInfo(long lineId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT * FROM line_account "
        + "WHERE line_id = " + lineId;
    ResultSet re = null;
    LineAccountInfo result = new LineAccountInfo();
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result.postUrl = re.getString(2);
        result.accessToken = re.getString(3);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return result;
  }
  
  /**
   * Twitterアカウント取得. 
   * @param twitterId TwitterID
   * @return TwitterアカウントObject
   */
  public static TwitterAccountInfo getTwitterAccountInfo(long twitterId) {
    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String sql = "SELECT * FROM twitter_account "
        + "WHERE twitter_id = " + twitterId;
    ResultSet re = null;
    TwitterAccountInfo result = new TwitterAccountInfo();
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      if (re.next()) {
        result.userName = re.getString(2);
        result.consumerkey = re.getString(3);
        result.consumerSec = re.getString(4);
        result.accessToken = re.getString(5);
        result.accessTokenSec = re.getString(6);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return result;
  }
  
  /**
   * 配信FAX情報取得. 
   * @param districtIdLst 区IDリスト
   * @return 利用対象の配信FAX情報リスト
   */
  public static ArrayList<FaxInfoEntity> getFaxInfo(ArrayList<Long> districtIdLst) {
    ArrayList<FaxInfoEntity> faxInfoLst = new ArrayList<FaxInfoEntity>();
    if (districtIdLst.size() == 0) {
      //対象無し
      return faxInfoLst;
    }

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      logger.warn("DB connect error.");
      return null;
    }
    // DB接続成功
    Statement statement = null;
    String disIdlist = districtIdLst.toString();
    String sql = "SELECT f.fax_no, f.fax_name "
        + "FROM district d "
        + "LEFT OUTER JOIN deliv_fax_no f ON d.district_id = f.district_id "
        + "WHERE district_type = " + Const.DISTRICT 
            + " AND d.district_id IN (" + disIdlist.substring(1, disIdlist.length() - 1) + ")";
    ResultSet re = null;
    try {
      statement = con.createStatement();
      // SQL文実行
      re = statement.executeQuery(sql);
      while (re.next()) {
        FaxInfoEntity result = new FaxInfoEntity();
        result.faxNo = re.getString(1);
        result.faxName = re.getString(2);
        faxInfoLst.add(result);
      }
    } catch (SQLException e) {
      logger.warn("SQL run error. " + e);
      // DBから取得異常
      jdbc.release(con, statement, re);
      return null;
    }
    jdbc.release(con, statement, re);
    return faxInfoLst;
  }
  
  /**
   * DIST_STATUSテーブルの通報制御IDを更新する. 
   * 
   * @param sql SQL文
   * @return 実行結果
   */
  public static boolean updateDistStatus(String sql) {

    // DB接続を行う
    Connection con = null;
    JdbcUtils jdbc = new JdbcUtils();
    con = jdbc.getConnection();
    if (con == null) {
      // DB接続失敗
      return false;
    }
    // DB接続成功
    Statement statement = null;
    ResultSet re = null;

    try {
      statement = con.createStatement();
      // SQL文実行
      if (statement.executeUpdate(sql) > 0) {
        jdbc.release(con, statement, re);
        return true;
      } else {
        jdbc.release(con, statement, re);
        return false;
      }
    } catch (SQLException e) {
      logger.warn(e);
      // DBから取得異常の場合、settingConfig.jsonから取得
      jdbc.release(con, statement, re);
      return false;
    }
  }

}