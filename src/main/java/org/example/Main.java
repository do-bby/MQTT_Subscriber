package org.example;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
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

    public static void main(String[] args) throws IOException {
        String brokerAddress = "tcp://139.150.83.249:1883";
        String username = "root";
        String password = "public";
        String topic = "PohangPG/"+uID+"/#";
        String clientId = "Sub";

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
                }
                @Override
                public void messageArrived(String topic, MqttMessage message) throws SQLException, IOException {
                    System.out.println("Message received:\n" + "\tTopic: " + topic + "\n\tMessage: " + new String(message.getPayload()));
                    insertDB(topic,message);
                    updateDB(topic,message);
                }
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("Delivery complete.");
                }
            });
        } catch (MqttException e) {
            e.printStackTrace();
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
        String databaseName = "emqx_data";
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
            ps.setString(1,uID);
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
        String updateQuery = "UPDATE emqx_messages_master SET payload = ?, created_at = ? WHERE clientid = ? AND topic = ?";
        String insertQuery = "INSERT INTO emqx_messages_master (clientid,topic,payload,created_at) VALUES (?,?,?,?)";
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
            ps.setString(1,uID);
            ps.setString(2, topic);
            rs = ps.executeQuery();
            if(rs.next()){
                //update
                PreparedStatement updateps = con.prepareStatement(updateQuery);
                updateps.setString(1, new String(message.getPayload()));
                updateps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                updateps.setString(3,uID);
                updateps.setString(4,topic);
                int rows = updateps.executeUpdate();
                if(rows > 0){
                    System.out.println("update success1");
                } else{
                    System.out.println("update fail1");
                }
            } else {
                PreparedStatement insertps = con.prepareStatement(insertQuery);
                insertps.setString(1,uID);
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
}