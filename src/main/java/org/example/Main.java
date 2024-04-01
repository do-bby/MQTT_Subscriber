package org.example;
import org.eclipse.paho.client.mqttv3.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.json.JSONArray;
import org.json.JSONObject;

public class Main {
    static String uID;
    static {
        try {
            uID = getUID();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    static String postData = "{\"userName\": \"openapiuser@gmail.com\", \"password\": \"AQAAAAEAACcQAAAAEGkDt7W5HYOCORwYFD0oZk3nG7R/klqVO6Zr5IKCEEsf9ElsGcysBvO0vAZsbPCd1g==\"}";
    public static void main(String[] args) throws IOException {
        String brokerAddress = "tcp://139.150.83.249:1883";
        String username = "emqx_guest";
        String password = "public";
        String topic = "PohangPG/#";
        String clientId = "Sub";
        int status = 0;
        //login api 호출
        URL url = new URL("https://pohang.ictpeople.co.kr/api/User/login");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = postData.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            JSONObject res = new JSONObject(response.toString());
            status = res.getInt("statusCode");
        }
        System.out.println(status);
        if(status == 200){
            try {
                MqttClient client = new MqttClient(brokerAddress, clientId);
                MqttConnectOptions conOpt = new MqttConnectOptions();
                conOpt.setCleanSession(true);
                conOpt.setUserName(username);
                conOpt.setPassword(password.toCharArray());
                client.connect(conOpt);
                System.out.println("Connected");
                client.subscribe(topic);

                //subscribe callback method
                client.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        System.out.println("Connection lost: " + cause.getMessage());
                        try {
                            reconnectToBroker();
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    private void reconnectToBroker() throws MqttException {
                        // 재연결 시도
                        client.reconnect();

                        // 연결 성공 메시지 출력
                        System.out.println("Reconnected to MQTT broker.");
                    }
                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws SQLException, IOException {
                        System.out.println("Message received:\n" + "\tTopic: " + topic + "\n\tMessage: " + new String(message.getPayload()));
                        insertDB(topic,message);
                        updateDB(topic,message);
                        //uID 기상이면 insertDB, updateDB할때 uID변경
                        if(topic.contains("Ncst")){
                            uncstInsert(topic,message);
                            Pohang.uncstInsert(topic, message);
                        }
                        else if(topic.contains("Fcst")){
                            ufcstInsert(topic, message);
                            Pohang.ufcstInsert(topic, message);
                        }
                        else if(topic.contains("VF")){
                            vfcstInsert(topic,message);
                            Pohang.vfcstInsert(topic, message);
                        }
                        else if(topic.contains("ocean")){
                            oceanInsert(topic, message);
                            Pohang.insertOcean(topic,message);
                        }
                        else if(topic.contains("tide")){
                            oceandataInsert(topic, message);
                            Pohang.oceandataInsert(topic, message);
                        }
                        else if(topic.contains("WthrWrnMsg")){
                            wthrWrnMsgInsert(topic, message);
                            Pohang.wthrWrnMsgInsert(topic, message);
                        }
                        else if(topic.contains("uv") || topic.contains("atmo")){
                            uvindexInsert(topic, message);
                            Pohang.uvindexInsert(topic, message);
                        }
                        else if(topic.contains("wave")){
                            waveInsert(topic, message);
                            Pohang.waveInsert(topic, message);
                        }
                        else if(topic.contains("PreTab")){
                            Pohang.tideObsPreTabInsert(topic,message);
                        }
                        else if(topic.contains("LunPhInfo")){
                            Pohang.lunphinfoInsert(topic,message);
                        }
                    }
                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        System.out.println("Delivery complete.");
                    }
                });
            } catch (MqttException e) {
                e.getMessage();
            }
        }
        else{
            System.out.println("Login fail");
        }
    }
    public static String Regex (String topic){
        Pattern pattern = Pattern.compile("/(.*?)/");
        Matcher matcher = pattern.matcher(topic);
        if(matcher.find()){
            return matcher.group(1);
        }
        else{
            return null;
        }
    }
    //Get serialNo
    public static String getUID() throws IOException {
        URL url = new URL("https://pohang.ictpeople.co.kr/api/Equipment/GetEquipment?SerialNo=DX20240220-0001");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        String uID = null;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream inputStream = connection.getInputStream();
            byte[] responseData = inputStream.readAllBytes();
            String response = new String(responseData);
            JSONObject jsonObject = new JSONObject(response);
            JSONArray array = jsonObject.getJSONArray("data");
            JSONObject obj = (JSONObject) array.get(0);
            uID = obj.getString("serialNo");
            //System.out.println(uID);

        } else {
            System.out.println("HTTP GET 요청 실패: " + responseCode);
        }
        return uID;
    }
    //Insert to table(emqx_messages) : Data history
    public static void insertDB(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "pohang";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO emqx_messages (clientid,topic,payload,created_at) VALUES (?,?,?,?)";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try{
            con = dataSource.getConnection();
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1,Regex(topic));
            ps.setString(2,topic);
            ps.setString(3, new String(message.getPayload()));
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));

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
    //Update to table(emqx_messages_master) : Recent Data
    public static void updateDB(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
        String username = "root";
        String password = "public";
        ResultSet rs =null;
        String checkQuery = "SELECT * FROM emqx_messages_master WHERE clientid = ? AND topic = ?";
        String updateQuery = "UPDATE emqx_messages_master SET payload = ?, latest_time = ? WHERE clientid = ? AND topic = ?";
        String insertQuery = "INSERT INTO emqx_messages_master (clientid,topic,payload,latest_time) VALUES (?,?,?,?)";
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        Connection con = null;
        try{
            con = dataSource.getConnection();
            PreparedStatement ps = con.prepareStatement(checkQuery);
            ps.setString(1,Regex(topic));
            ps.setString(2, topic);
            rs = ps.executeQuery();
            if(rs.next()){
                //update
                PreparedStatement updateps = con.prepareStatement(updateQuery);
                updateps.setString(1, new String(message.getPayload()));
                updateps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                updateps.setString(3,Regex(topic));
                updateps.setString(4,topic);
                int rows = updateps.executeUpdate();
                if(rows > 0){
                    System.out.println("update success1");
                } else{
                    System.out.println("update fail1");
                }
            } else {
                PreparedStatement insertps = con.prepareStatement(insertQuery);
                insertps.setString(1,Regex(topic));
                insertps.setString(2,topic);
                insertps.setString(3, new String(message.getPayload()));
                insertps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
                int rows = insertps.executeUpdate();
                if(rows > 0){
                    System.out.println("update success2");
                } else{
                    System.out.println("update fail2");
                }
            }

        } catch(SQLException e){
            System.out.println("error : " + e.getMessage());
        }finally {
            if(con != null){
                con.close();
            }
        }
    }
    //test UNCST (초단기 실황)
    public static void uncstInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
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
        String databaseName = "emqx_data";
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
            System.out.println("1"+data);
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, data.getString("fcstValue"));
            ps.setString(2, data.getString("fcstDate"));
            ps.setString(3, data.getString("fcstTime"));
            ps.setString(4, data.getString("baseDate"));
            ps.setString(5, data.getString("baseTime"));
            ps.setInt(6, data.getInt("nx"));
            ps.setInt(7, data.getInt("ny"));
            ps.setString(8, data.getString("category"));
            System.out.println("1"+data.getString("category"));
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
        String databaseName = "emqx_data";
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

    public static void oceanInsert(String topic, MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
        String username = "root";
        String password = "public";
        String query = "INSERT INTO API_Oceandata (recordtime,obsid,obsname,lat,lon,obs_last_req,air_temp,air_press,tide_level,water_temp,wind_dir,wind_gust,wind_speed,Salinity, modified, created) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE " +
                "obsname = VALUES(obsname), lat = VALUES(lat), lon = VALUES(lon), obs_last_req = VALUES(obs_last_req), " +
                "air_temp = VALUES(air_temp), air_press = VALUES(air_press), tide_level = VALUES(tide_level), " +
                "water_temp = VALUES(water_temp), wind_dir = VALUES(wind_dir), wind_gust = VALUES(wind_gust), " +
                "wind_speed = VALUES(wind_speed), Salinity = VALUES(Salinity), modified = NOW()";
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
            ps.setString(1, data.getJSONObject("data").getString("record_time"));
            ps.setString(2, data.getJSONObject("meta").getString("obs_post_id"));
            ps.setString(3, data.getJSONObject("meta").getString("obs_post_name"));
            ps.setString(4, data.getJSONObject("meta").getString("obs_lat"));
            ps.setString(5, data.getJSONObject("meta").getString("obs_lon"));
            ps.setString(6, data.getJSONObject("meta").getString("obs_last_req_cnt"));
            ps.setString(7, data.getJSONObject("data").getString("air_temp"));
            ps.setString(8, data.getJSONObject("data").getString("air_press"));
            ps.setString(9, data.getJSONObject("data").getString("tide_level"));
            ps.setString(10, data.getJSONObject("data").getString("water_temp"));
            ps.setString(11, data.getJSONObject("data").getString("wind_dir"));
            ps.setString(12, data.getJSONObject("data").getString("wind_gust"));
            ps.setString(13, data.getJSONObject("data").getString("wind_speed"));
            ps.setString(14, data.getJSONObject("data").getString("Salinity"));

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

    public static void oceandataInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
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
        String databaseName = "emqx_data";
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
    public static void uvindexInsert(String topic,MqttMessage message) throws SQLException, IOException {
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
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
        String serverName = "139.150.83.249";
        int port = 3306;
        String databaseName = "emqx_data";
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
}