package org.example;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class Pohang {
    public static void insertOcean(String topic, MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        String query = "INSERT INTO API_ObsRecent (OBS_POST_ID,OBS_POST_NM,OBS_LAT,OBS_LON,OBS_LAST_REQ_CNT,RECODE_TIME,WIND_DIR,WIND_SPEED,WIND_GUST,AIR_TEMP,AIR_PRESS,WATER_TEMP,TIDE_LEVEL,SALINITY,CreatedDate,CreatedBy,ModifiedDate,ModifiedBy) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?,NOW(),?)" +
                "ON DUPLICATE KEY UPDATE OBS_POST_NM = VALUES(OBS_POST_NM), OBS_LAT = VALUES(OBS_LAT),OBS_LON = VALUES(OBS_LON),OBS_LAST_REQ_CNT = VALUES(OBS_LAST_REQ_CNT),WIND_DIR = VALUES(WIND_DIR),WIND_SPEED = VALUES(WIND_SPEED),WIND_GUST = VALUES(WIND_GUST),AIR_TEMP = VALUES(AIR_TEMP),AIR_PRESS = VALUES(AIR_PRESS),WATER_TEMP = VALUES(WATER_TEMP),TIDE_LEVEL = VALUES(TIDE_LEVEL),SALINITY = VALUES(SALINITY)," +
                "ModifiedDate = NOW()";
        if(topic.contains("ocean")){
            try {
                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("meta").getString("obs_last_req_cnt"));
                ps.setString(6, data.getJSONObject("data").getString("record_time"));
                ps.setString(7, data.getJSONObject("data").getString("wind_dir"));
                ps.setString(8, data.getJSONObject("data").getString("wind_speed"));
                ps.setString(9, data.getJSONObject("data").getString("wind_gust"));
                ps.setString(10, data.getJSONObject("data").getString("air_temp"));
                ps.setString(11, data.getJSONObject("data").getString("air_press"));
                ps.setString(12, data.getJSONObject("data").getString("water_temp"));
                ps.setString(13, data.getJSONObject("data").getString("tide_level"));
                ps.setString(14, data.getJSONObject("data").getString("Salinity"));
                ps.setString(15, "admin");
                ps.setString(16, "admin");

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            } catch (SQLException e) {
                System.out.println("error : " + e.getMessage());
            } finally {
                if (con != null) {
                    con.close();
                }
            }
        }
    }
    public static void uvindexInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try {
            if (topic.contains("uv")) {
                String query = "INSERT INTO API_UVINDEX (code, areaNo, date, msg,created,modified) VALUES (?, ?, ?, ?,NOW(),NOW()) " +
                        "ON DUPLICATE KEY UPDATE code = VALUES(code), msg = VALUES(msg)" +
                        ", modified = NOW()";
                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getString("code"));
                ps.setString(2, data.getString("areaNo"));
                ps.setString(3, data.getString("date"));
                ps.setString(4, data.toString());
                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            }
            else if (topic.contains("atmo")) {
                String query = "INSERT INTO API_ATMO (code, areaNo, date, msg,created,modified) VALUES (?, ?, ?, ?,NOW(),NOW()) " +
                        "ON DUPLICATE KEY UPDATE code = VALUES(code), msg = VALUES(msg)" +
                        ", modified = NOW()";
                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getString("code"));
                ps.setString(2, data.getString("areaNo"));
                ps.setString(3, data.getString("date"));
                ps.setString(4, data.toString());
                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            }
        }catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void waveInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249"
                ;
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try {
            String query = "INSERT INTO API_WAVE (beachNum, tm, wh,created,modified) VALUES (?, ?, ?, NOW(),NOW()) " +
                    "ON DUPLICATE KEY UPDATE tm = VALUES(tm), wh = VALUES(wh)" +
                    ", modified = NOW()";
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, data.getString("beachNum"));
            ps.setString(2, data.getString("tm"));
            ps.setString(3, data.getString("wh"));
            int rows = ps.executeUpdate();
            if (rows > 0) {
                System.out.println("insert success");
            } else {
                System.out.println("insert fail");
            }

        }catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void uncstInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO API_UltraSrtNcst (obsrvalue, basedate, basetime, nx, ny, category,created) VALUES (?, ?, ?, ?, ?, ?,NOW()) " +
                "ON DUPLICATE KEY UPDATE obsrvalue = VALUES(obsrvalue), nx = VALUES(nx), ny = VALUES(ny), " +
                "modified = NOW()";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try{
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1,data.getString("obsrValue"));
            ps.setString(2,data.getString("baseDate"));
            ps.setString(3, data.getString("baseTime"));
            ps.setInt(4, data.getInt("nx"));
            ps.setInt(5, data.getInt("ny"));
            ps.setString(6, data.getString("category"));
            int rows = ps.executeUpdate();
            if(rows > 0){
                System.out.println("insert success");
            } else{
                System.out.println("insert fail");
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void ufcstInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO API_UltraSrtFcst (fcstvalue, fcstdate, fcsttime, basedate, basetime, nx, ny, category,created,modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?,NOW(),NOW()) " +
                "ON DUPLICATE KEY UPDATE fcstvalue = VALUES(fcstvalue), fcsttime = VALUES(fcsttime), fcstdate = VALUES(fcstdate), nx = VALUES(nx), ny = VALUES(ny)" +
                ", modified = NOW()";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try {
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, data.getString("fcstValue"));
            ps.setString(2, data.getString("fcstDate"));
            ps.setString(3, data.getString("fcstTime"));
            ps.setString(4, data.getString("baseDate"));
            ps.setString(5, data.getString("baseTime"));
            ps.setInt(6, data.getInt("nx"));
            ps.setInt(7, data.getInt("ny"));
            ps.setString(8, data.getString("category"));
            int rows = ps.executeUpdate();
            if(rows > 0) {
                System.out.println("insert success");
            } else {
                System.out.println("insert fail");
            }
        } catch(SQLException e) {
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null) {
                con.close();
            }
        }
    }
    public static void vfcstInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO API_VilageFcst (fcstvalue, fcstdate, fcsttime, basedate, basetime, nx, ny, category,created,modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?,NOW(),NOW()) " +
                "ON DUPLICATE KEY UPDATE fcstvalue = VALUES(fcstvalue), fcsttime = VALUES(fcsttime), fcstdate = VALUES(fcstdate), nx = VALUES(nx), ny = VALUES(ny) " +
                ", modified = NOW()";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try{
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1,data.getString("fcstValue"));
            ps.setString(2,data.getString("fcstDate"));
            ps.setString(3, data.getString("fcstTime"));
            ps.setString(4,data.getString("baseDate"));
            ps.setString(5, data.getString("baseTime"));
            ps.setInt(6, data.getInt("nx"));
            ps.setInt(7, data.getInt("ny"));
            ps.setString(8, data.getString("category"));
            System.out.println("insert success");
            int rows = ps.executeUpdate();
            if(rows > 0){
                System.out.println("insert success");
            } else{
                System.out.println("insert fail");
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void oceandataInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;

        try{
            if (topic.contains("tideCurPre")) {
                String query = "INSERT INTO API_tideCurPre (obs_id, obs_name, obs_lat, obs_lon, record_time, real_value, pre_value, obs_last_req_cnt, modified, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "real_value = VALUES(real_value), pre_value = VALUES(pre_value), obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";

                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("data").getString("record_time"));
                ps.setString(6, data.getJSONObject("data").getString("real_value"));
                ps.setString(7, data.getJSONObject("data").getString("pre_value"));
                ps.setString(8, data.getJSONObject("meta").getString("obs_last_req_cnt"));

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            } else if (topic.contains("tideObsAirPres")) {
                String query = "INSERT INTO API_tideObsAirPres (obs_id, obs_name, obs_lat, obs_lon, record_time, air_pres, obs_last_req_cnt, modified, created) VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "air_pres = VALUES(air_pres), obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";

                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("data").getString("record_time"));
                ps.setString(6, data.getJSONObject("data").getString("air_pres"));
                ps.setString(7, data.getJSONObject("meta").getString("obs_last_req_cnt"));

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            } else if(topic.contains("tideObsAirTemp")){
                String query = "INSERT INTO API_tideObsAirTemp (obs_id, obs_name, obs_lat, obs_lon, record_time, air_temp, obs_last_req_cnt, modified, created) VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "air_temp = VALUES(air_temp), obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";
                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1,data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2,data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3,data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4,data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5,data.getJSONObject("data").getString("record_time"));
                ps.setString(6,data.getJSONObject("data").getString("air_temp"));
                ps.setString(7,data.getJSONObject("meta").getString("obs_last_req_cnt"));
                int rows = ps.executeUpdate();
                if(rows > 0){
                    System.out.println("insert success");
                } else{
                    System.out.println("insert fail");
                }
            }
            else if (topic.contains("tideObsSalt")) {
                String query = "INSERT INTO API_tideObsSalt (obs_id, obs_name, obs_lat, obs_lon, record_time, salinity, obs_last_req_cnt, modified, created) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "salinity = VALUES(salinity), obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";

                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("data").getString("record_time"));
                ps.setString(6, data.getJSONObject("data").getString("salinity"));
                ps.setString(7, data.getJSONObject("meta").getString("obs_last_req_cnt"));

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            }
            else if (topic.contains("tideObsTemp")) {
                String query = "INSERT INTO API_tideObsTemp (obs_id, obs_name, obs_lat, obs_lon, record_time, water_temp, obs_last_req_cnt, modified, created) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "record_time = VALUES(record_time), water_temp = VALUES(water_temp), obs_last_req_cnt = VALUES(obs_last_req_cnt), " +
                        "modified = NOW()";

                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("data").getString("record_time"));
                ps.setString(6, data.getJSONObject("data").getString("water_temp"));
                ps.setString(7, data.getJSONObject("meta").getString("obs_last_req_cnt"));

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            }
            else if (topic.contains("tideObsWind")) {
                String query = "INSERT INTO API_tideObsWind (obs_id, obs_name, obs_lat, obs_lon, record_time, wind_dir, wind_speed, obs_last_req_cnt, modified, created) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "record_time = VALUES(record_time), wind_dir = VALUES(wind_dir), wind_speed = VALUES(wind_speed), " +
                        "obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";

                con = dataSource.getConnection();
                JSONObject data = new JSONObject(new String(message.getPayload()));
                PreparedStatement ps = con.prepareStatement(query);
                ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
                ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
                ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
                ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
                ps.setString(5, data.getJSONObject("data").getString("record_time"));
                ps.setString(6, data.getJSONObject("data").getString("wind_dir"));
                ps.setString(7, data.getJSONObject("data").getString("wind_speed"));
                ps.setString(8, data.getJSONObject("meta").getString("obs_last_req_cnt"));

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    System.out.println("insert success");
                } else {
                    System.out.println("insert fail");
                }
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void wthrWrnMsgInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO API_WthrWrnMsg (stnId, tmFc, tmSeq, area, warFc, title, msg, Fermentationtime, Specialfermentationtime, Specialmsg,Prespecialreport,other,created,modified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?, ?, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE stnId = VALUES(stnId), tmSeq = VALUES(tmSeq), area = VALUES(area), " +
                "warFc = VALUES(warFc), title = VALUES(title), msg = VALUES(msg), " +
                "Fermentationtime = VALUES(Fermentationtime), Specialfermentationtime = VALUES(Specialfermentationtime), Specialmsg = VALUES(Specialmsg), Prespecialreport = VALUES(Prespecialreport), other = VALUES(other), modified = NOW()";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try{
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1,data.getString("stnId"));
            Object obj = data.get("tmFc");
            String d = String.valueOf(obj);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
            Date parsedDate = dateFormat.parse(d);
            Timestamp tmFcTimestamp = new Timestamp(parsedDate.getTime());
            ps.setTimestamp(2, tmFcTimestamp);
            ps.setInt(3,data.getInt("tmSeq"));
            ps.setString(4,data.getString("t2"));
            ps.setString(5,data.getString("warFc"));
            ps.setString(6,data.getString("t1"));
            ps.setString(7,data.getString("t4"));
            ps.setString(8,data.getString("t3"));
            Object obj2 = data.get("t5");
            String d2 = String.valueOf(obj2);
            Date parsedDate2 = dateFormat.parse(d2);
            Timestamp tmFcTimestamp2 = new Timestamp(parsedDate2.getTime());
            ps.setTimestamp(9,tmFcTimestamp2);
            ps.setString(10,data.getString("t6"));
            ps.setString(11,data.getString("t7"));
            ps.setString(12,data.getString("other"));
            int rows = ps.executeUpdate();
            if(rows > 0){
                System.out.println("insert success");
            } else{
                System.out.println("insert fail");
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            if(con != null){
                con.close();
            }
        }
    }
    public static void tideObsPreTabInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        String query = "INSERT INTO API_tideObsPreTab (obs_id, obs_name, obs_lat, obs_lon, tph_time, tph_level, hl_code, obs_last_req_cnt, modified, created) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                "tph_time = VALUES(tph_time), tph_level = VALUES(tph_level), hl_code = VALUES(hl_code), " +
                "obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";
        try{
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, data.getJSONObject("meta").getString("obs_post_id"));
            ps.setString(2, data.getJSONObject("meta").getString("obs_post_name"));
            ps.setString(3, data.getJSONObject("meta").getString("obs_lat"));
            ps.setString(4, data.getJSONObject("meta").getString("obs_lon"));
            ps.setString(5, data.getJSONObject("data").getString("tph_time"));
            ps.setString(6, data.getJSONObject("data").getString("tph_level"));
            ps.setString(7, data.getJSONObject("data").getString("hl_code"));
            ps.setString(8, data.getJSONObject("meta").getString("obs_last_req_cnt"));
            System.out.println("insert success");
            int rows = ps.executeUpdate();
            if(rows > 0){
                System.out.println("insert success");
            } else{
                System.out.println("insert fail");
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } finally {
            if(con != null){
                con.close();
            }
        }


    }

    public static void lunphinfoInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        String query = "INSERT INTO API_lunPhInfo (lunAge,sol_time, modified, created) " +
                "VALUES (?, ?, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE lunAge = VALUES(lunAge), sol_time = VALUES(sol_time),modified = NOW()";
        try{
            con = dataSource.getConnection();
            JSONObject data = new JSONObject(new String(message.getPayload()));

            PreparedStatement ps = con.prepareStatement(query);
            ps.setDouble(1, data.getDouble("lunAge"));
            System.out.println(data.getDouble("lunAge"));

            int year = data.getInt("solYear");
            int month = data.getInt("solMonth");
            int day = data.getInt("solDay");

            String sol = String.format("%04d%02d%02d", year, month, day);
            System.out.println();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            Date parsedDate = dateFormat.parse(sol);
            System.out.println(parsedDate);
            Timestamp timestamp = new Timestamp(parsedDate.getTime());

            ps.setTimestamp(2, timestamp);
            System.out.println("insert success");
            int rows = ps.executeUpdate();
            if(rows > 0){
                System.out.println("insert success");
            } else{
                System.out.println("insert fail");
            }
        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            if(con != null){
                con.close();
            }
        }


    }
}
