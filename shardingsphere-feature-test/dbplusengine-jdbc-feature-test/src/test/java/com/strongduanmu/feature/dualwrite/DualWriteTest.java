package com.strongduanmu.feature.dualwrite;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

/**
 * Dual write test.
 */
@Slf4j
public class DualWriteTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException, IOException {
        DataSource dataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.DUAL_WRITE);
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testCreateTable() throws SQLException {
        String sql = "CREATE TABLE \"GTCRCOA\".\"OA_INCOMING_DISPATCHES\" (\n" + "  \"SYS_ID\" VARCHAR2(32 CHAR)  NOT NULL ,\n" + "  \"DOCUMENT_IDENTIFIER\" VARCHAR2(64 CHAR)  ,\n" + "  \"ISSUED_NUMBER_OF_DOCUMENT\" VARCHAR2(64 CHAR)  ,\n" + "  \"DOCUMENT_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"SERIAL_NUMBER_OF_COPIES\" VARCHAR2(10 CHAR)  ,\n" + "  \"EMERGENCY_DEGREE\" VARCHAR2(64 CHAR)  ,\n" + "  \"DOCUMENT_TITLE\" VARCHAR2(256 CHAR)  NOT NULL ,\n" + "  \"ANNOTATION\" VARCHAR2(4000 CHAR)  ,\n" + "  \"INSTANCE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"SERIAL_NUMBER\" VARCHAR2(128 CHAR)  ,\n" + "  \"DOCUMENT_TIME\" TIMESTAMP(6)  ,\n" + "  \"IS_ARCHIVED\" VARCHAR2(10 CHAR)  ,\n" + "  \"ARCHIVED_DATE\" TIMESTAMP(6)  ,\n" + "  \"IS_FINISH\" VARCHAR2(10 CHAR)  ,\n" + "  \"FINISH_DATE\" TIMESTAMP(6)  ,\n" + "  \"DOCUMENT_WORKFLOW_CODE\" VARCHAR2(32 CHAR)  ,\n" + "  \"DOCUMENT_MAIN_ATTACHMENT_ID\" VARCHAR2(128 CHAR)  ,\n" + "  \"FROM_DEPARTMENT\" VARCHAR2(128 CHAR)  ,\n" + "  \"REGISTER_NAME\" VARCHAR2(64 CHAR)  ,\n" + "  \"PROCESSING_TIME_LIMIT\" DATE  ,\n" + "  \"ATTACHMENT_THICKENING\" VARCHAR2(64 CHAR)  ,\n" + "  \"MODIFY_DATE\" TIMESTAMP(6)  ,\n" + "  \"CREATE_TIME\" TIMESTAMP(6)  ,\n" + "  \"ATTACHMENT_NAME\" VARCHAR2(1024 BYTE)  DEFAULT '' ,\n" + "  \"ATTACHMENT_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"FONT_STYLE\" VARCHAR2(32 CHAR)  ,\n" + "  \"YEAR\" VARCHAR2(6 CHAR)  DEFAULT '' ,\n" + "  \"UNIT\" VARCHAR2(6 CHAR)  DEFAULT NULL ,\n" + "  \"NUMBER_ID\" VARCHAR2(32 CHAR)  ,\n" + "  \"SERIAL_NO\" VARCHAR2(32 CHAR)  ,\n" + "  \"REGISTER_ID\" VARCHAR2(32 CHAR)  ,\n" + "  \"DEPART_TITLE_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"PER_FORM_URL\" VARCHAR2(128 CHAR)  ,\n" + "  \"DOCUMENT_HEADER\" VARCHAR2(128 CHAR)  ,\n" + "  \"FENFA_OR_SIGN\" VARCHAR2(36 CHAR)  ,\n" + "  \"FLOW_SERIAL_NUMBER\" VARCHAR2(64 CHAR)  ,\n" + "  \"BARCODE\" VARCHAR2(2000 CHAR)  ,\n" + "  \"CATEGORY_ID\" VARCHAR2(34 BYTE)  ,\n" + "  \"IS_LOCK\" VARCHAR2(6 CHAR)  ,\n" + "  \"LOCK_USER_MESSAGE\" VARCHAR2(128 CHAR)  ,\n" + "  \"RECYCLE_FLAG\" VARCHAR2(2 BYTE)  ,\n" + "  \"DEPART_TITLE_TYPE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"INSIDE_STATE\" VARCHAR2(6 CHAR)  ,\n" + "  \"REMARK\" VARCHAR2(6 BYTE)  ,\n" + "  \"IS_DIFFERENTIATE\" VARCHAR2(6 CHAR)  ,\n" + "  \"IS_LINGDAO_TYPE\" VARCHAR2(6 CHAR)  \n" + ")";
        Statement statement = connection.createStatement();
        statement.execute(sql);
    
        sql = "CREATE TABLE \"GTCRCOA\".\"OA_OUTGOING_MESSAGE\" (\n" + "  \"SYS_ID\" VARCHAR2(32 CHAR)  DEFAULT NULL NOT NULL ,\n" + "  \"DOCUMENT_IDENTIFIER\" VARCHAR2(64 CHAR)  ,\n" + "  \"DOCUMENT_TYPE\" VARCHAR2(64 CHAR)  ,\n" + "  \"SERIAL_NUMBER_OF_COPIES\" VARCHAR2(10 CHAR)  ,\n" + "  \"EMERGENCY_DEGREE\" VARCHAR2(32 CHAR)  ,\n" + "  \"DOCUMENT_TITLE\" VARCHAR2(256 CHAR)  ,\n" + "  \"ISSUED_NUMBER_OF_DOCUMENT\" VARCHAR2(64 CHAR)  ,\n" + "  \"SIGNER\" VARCHAR2(64 CHAR)  ,\n" + "  \"SIGNATURE_OF_DOCUMENT_ISSUING\" VARCHAR2(128 CHAR)  ,\n" + "  \"ISSUED_DATE_OF_DOCUMENT\" TIMESTAMP(6)  ,\n" + "  \"ANNOTATION\" VARCHAR2(4000 CHAR)  ,\n" + "  \"ATTACHMENT\" VARCHAR2(2048 CHAR)  ,\n" + "  \"INSTANCE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"DOCUMENT_TIME\" TIMESTAMP(6)  ,\n" + "  \"IS_ARCHIVED\" VARCHAR2(10 CHAR)  ,\n" + "  \"ARCHIVED_DATE\" TIMESTAMP(6)  ,\n" + "  \"IS_FINISH\" VARCHAR2(10 CHAR)  ,\n" + "  \"FINISH_DATE\" TIMESTAMP(6)  ,\n" + "  \"DOCUMENT_WORKFLOW_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"DRAFTER_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"DRAFTER_NAME\" VARCHAR2(64 CHAR)  ,\n" + "  \"DRAFTER_TEL\" VARCHAR2(32 CHAR)  ,\n" + "  \"DRAFTER_DEPT\" VARCHAR2(64 CHAR)  ,\n" + "  \"IS_NEED_LEGALITY_CHECK\" VARCHAR2(10 CHAR)  ,\n" + "  \"IS_IMPORTANT_FILE\" VARCHAR2(10 CHAR)  ,\n" + "  \"IS_TECHNOLOGY_FILE\" VARCHAR2(10 CHAR)  ,\n" + "  \"RELEVANCE_DOCUMENT\" VARCHAR2(128 CHAR)  ,\n" + "  \"SIGN_DEPARTMENT\" VARCHAR2(2000 CHAR)  ,\n" + "  \"ISSUING_DEPARTMENT\" VARCHAR2(2000 CHAR)  ,\n" + "  \"ATTACHMENT_THICKENING\" VARCHAR2(64 CHAR)  ,\n" + "  \"DRAFTER_CREATE_TIME\" TIMESTAMP(6)  ,\n" + "  \"MODIFY_DATE\" TIMESTAMP(6)  ,\n" + "  \"FILE_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"ATTACHMENT_NAME\" VARCHAR2(1024 BYTE)  ,\n" + "  \"ATTACHMENT_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"FONT_STYLE\" VARCHAR2(32 CHAR)  ,\n" + "  \"YEAR\" VARCHAR2(6 CHAR)  ,\n" + "  \"UNIT\" VARCHAR2(6 CHAR)  DEFAULT NULL ,\n" + "  \"NUMBER_ID\" VARCHAR2(32 CHAR)  ,\n" + "  \"SERIAL_NO\" VARCHAR2(32 CHAR)  ,\n" + "  \"SERIAL_NUMBER\" VARCHAR2(128 CHAR)  ,\n" + "  \"DEPART_TITLE_TYPE\" VARCHAR2(32 CHAR)  ,\n" + "  \"DRAFTER_PHONE\" VARCHAR2(32 CHAR)  ,\n" + "  \"STAMP_TYPESETTING\" VARCHAR2(32 CHAR)  ,\n" + "  \"FILE_TYPE_PRE\" VARCHAR2(32 CHAR)  ,\n" + "  \"PER_FORM_URL\" VARCHAR2(128 CHAR)  ,\n" + "  \"AIGANTURE_ID\" VARCHAR2(32 CHAR)  ,\n" + "  \"FILE_TYPE_ALL\" VARCHAR2(256 CHAR)  ,\n" + "  \"MAIN_TODO_DEPARTMENT\" VARCHAR2(1000 CHAR)  ,\n" + "  \"MAIN_TODO_DEPARTMENT_ID\" VARCHAR2(2000 CHAR)  ,\n" + "  \"DOCUMENT_HEADER\" VARCHAR2(128 CHAR)  ,\n" + "  \"ISSUING_DATE\" TIMESTAMP(6)  ,\n" + "  \"FLOW_SERIAL_NUMBER\" VARCHAR2(64 CHAR)  ,\n" + "  \"BARCODE\" VARCHAR2(2000 CHAR)  ,\n" + "  \"IS_LOCK\" VARCHAR2(6 CHAR)  ,\n" + "  \"LOCK_USER_MESSAGE\" VARCHAR2(128 CHAR)  ,\n" + "  \"MAIN_RECEIVER_DEPARTMENT\" CLOB  ,\n" + "  \"COPY_TO_DEPARTMENT\" CLOB  ,\n" + "  \"MAIN_RECEIVER_DEPT_ID\" CLOB  ,\n" + "  \"COPY_TO_DEPT_ID\" CLOB  ,\n" + "  \"IS_BUSSBACK_COMMENTS\" VARCHAR2(4000 CHAR)  ,\n" + "  \"QIANFA_A\" VARCHAR2(128 BYTE)  ,\n" + "  \"QIANFA_B\" VARCHAR2(128 BYTE)  ,\n" + "  \"QIANFA_C\" VARCHAR2(128 BYTE)  ,\n" + "  \"QIANFA_D\" VARCHAR2(128 BYTE)  ,\n" + "  \"HUIQIAN_A\" VARCHAR2(128 BYTE)  ,\n" + "  \"HUIQIAN_B\" VARCHAR2(128 BYTE)  ,\n" + "  \"HUIQIAN_C\" VARCHAR2(128 BYTE)  ,\n" + "  \"HUIQIAN_D\" VARCHAR2(128 BYTE)  ,\n" + "  \"RECYCLE_FLAG\" VARCHAR2(2 BYTE)  ,\n" + "  \"DEPART_TITLE_TYPE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"INSIDE_STATE\" VARCHAR2(6 CHAR)  ,\n" + "  \"RECOMMENDATION\" VARCHAR2(512 CHAR)  ,\n" + "  \"IS_DIFFERENTIATE\" VARCHAR2(6 CHAR)  ,\n" + "  \"DRAFTER_LOCATION\" VARCHAR2(32 CHAR)  ,\n" + "  \"SIGN_DEPARTMENT_NAME\" VARCHAR2(2000 CHAR)  ,\n" + "  \"ISSUING_DEPARTMENT_NAME\" VARCHAR2(2000 CHAR)  ,\n" + "  \"ZHANDUAN\" VARCHAR2(32 CHAR)  ,\n" + "  \"OPEN_WAY\" VARCHAR2(64 CHAR)  ,\n" + "  \"ATTRIBUTE1\" VARCHAR2(64 BYTE)  ,\n" + "  \"ATTRIBUTE2\" VARCHAR2(64 BYTE)  ,\n" + "  \"IS_LINGDAO_TYPE\" VARCHAR2(6 CHAR)  ,\n" + "  \"ZZBS\" VARCHAR2(6 CHAR)  ,\n" + "  \"SWGD\" VARCHAR2(6 CHAR)  ,\n" + "  \"ZXJS\" VARCHAR2(6 CHAR)  ,\n" + "  \"ZZQX\" VARCHAR2(6 CHAR)  ,\n" + "  \"CXZF\" VARCHAR2(6 CHAR)  ,\n" + "  \"ZBSJ\" VARCHAR2(6 CHAR)  ,\n" + "  \"GSGF\" VARCHAR2(6 CHAR)  ,\n" + "  \"CXSX\" VARCHAR2(6 CHAR)  ,\n" + "  \"ZQSJBM\" VARCHAR2(6 CHAR)  ,\n" + "  \"MAIN_RECEIVER_DEPARTMENT_SS_ID\" CLOB  ,\n" + "  \"COPY_TO_DEPARTMENT_SS_ID\" CLOB  ,\n" + "  \"MAIN_RECEIVER_DEPARTMENT_SS\" CLOB  ,\n" + "  \"COPY_TO_DEPARTMENT_SS\" CLOB  ,\n" + "  \"DRAFTER_DEPT_ID\" VARCHAR2(36 BYTE)  ,\n" + "  \"SECRET\" VARCHAR2(64 BYTE)  ,\n" + "  \"SECURITY_DEADLINE\" VARCHAR2(64 BYTE)  ,\n" + "  \"FILE_ATTRIBUTE\" VARCHAR2(64 BYTE)  ,\n" + "  \"CLASSIFY\" VARCHAR2(64 BYTE)  ,\n" + "  \"RETAINED_COPIES\" VARCHAR2(64 BYTE)  ,\n" + "  \"WRITING_DIRECT\" VARCHAR2(64 BYTE)  ,\n" + "  \"RECORD_TYPE\" VARCHAR2(64 BYTE)  ,\n" + "  \"OFFLINE_IDENTITY\" VARCHAR2(64 BYTE)  ,\n" + "  \"IS_CS_LOCK\" VARCHAR2(6 BYTE)  DEFAULT NULL ,\n" + "  \"DOCUMENT_MAIN_ATTACHMENT_ID\" VARCHAR2(100 BYTE)  ,\n" + "  \"IS_BEIAN\" VARCHAR2(6 BYTE)  \n" + ")";
        statement.execute(sql);
        
        sql = "CREATE TABLE \"GTCRCOA\".\"OA_WORKITEMS\" (\n" + "  \"SYS_ID\" VARCHAR2(36 CHAR)  NOT NULL ,\n" + "  \"INSTANCE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"INSTANCE_NAME\" VARCHAR2(256 CHAR)  ,\n" + "  \"WORKITEM_TYPE\" VARCHAR2(64 CHAR)  ,\n" + "  \"ACTIVITY_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"WORKFLOW_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"DISPLAY_NAME\" VARCHAR2(64 CHAR)  ,\n" + "  \"STATE\" VARCHAR2(10 CHAR)  ,\n" + "  \"RECEIVE_USER_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"RECEIVE_USER_NAME\" VARCHAR2(64 CHAR)  ,\n" + "  \"SUBMIT_TIME\" TIMESTAMP(6)  ,\n" + "  \"SUBMIT_USER_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"SUBMIT_USER_NAME\" VARCHAR2(64 CHAR)  ,\n" + "  \"FINISH_TIME\" TIMESTAMP(6)  ,\n" + "  \"SOURCE_WORKITEM_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"SOURCE_ACTIVITY_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"PARENT_INSTANCE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"ROOT_INSTANCE_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"MOBILE_OR_PC\" VARCHAR2(32 CHAR)  ,\n" + "  \"WORKITEM_ID\" VARCHAR2(36 CHAR)  ,\n" + "  \"CREATE_TIME\" TIMESTAMP(6)  ,\n" + "  \"CREATOR_ID\" VARCHAR2(64 CHAR)  ,\n" + "  \"CREATOR_CODE\" VARCHAR2(64 CHAR)  ,\n" + "  \"IS_READ\" VARCHAR2(10 CHAR)  ,\n" + "  \"TEXT_FIELD\" VARCHAR2(256 CHAR)  ,\n" + "  \"READ_TIME\" TIMESTAMP(6)  ,\n" + "  \"CANCEL_TIME\" TIMESTAMP(6)  ,\n" + "  \"IS_RETURN\" VARCHAR2(32 CHAR)  ,\n" + "  \"TRANSACTOR\" VARCHAR2(32 CHAR)  \n" + ")";
        statement.execute(sql);
    
        sql = "INSERT INTO \"OA_WORKITEMS\"(\"SYS_ID\", \"INSTANCE_ID\", \"INSTANCE_NAME\", \"WORKITEM_TYPE\", \"ACTIVITY_CODE\", \"WORKFLOW_CODE\", \"DISPLAY_NAME\", \"STATE\", \"RECEIVE_USER_CODE\", \"RECEIVE_USER_NAME\", \"SUBMIT_TIME\", \"SUBMIT_USER_CODE\", \"SUBMIT_USER_NAME\", \"FINISH_TIME\", \"SOURCE_WORKITEM_ID\", \"SOURCE_ACTIVITY_CODE\", \"PARENT_INSTANCE_ID\", \"ROOT_INSTANCE_ID\", \"MOBILE_OR_PC\", \"WORKITEM_ID\", \"CREATE_TIME\", \"CREATOR_ID\", \"CREATOR_CODE\", \"IS_READ\", \"TEXT_FIELD\", \"READ_TIME\", \"CANCEL_TIME\", \"IS_RETURN\", \"TRANSACTOR\") VALUES ('493bb8c5444a4ff8863eb7d25bf331f4', '52283ca3-f868-4143-bbc3-fd3dfe6e13b0', 'dlj会议通知测试1', 'ToDo', 'Activity2', 'zgs_hytzsw', '秘书', 'Finished', '69e8ab3726da4aa79fa5266acfe4a53d', '岳超', TO_TIMESTAMP('2022-06-24 05:27:12.000000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), 'c075ef61f0f545efbe929fc0bbca9321', '陆东福', TO_TIMESTAMP('2022-06-24 05:27:44.000000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), 'a5c857f7-5477-4a2c-96ba-86d1821f118a', 'Activity3', '9821d18e-fb33-4ce1-b203-4762532ac97a', '52283ca3-f868-4143-bbc3-fd3dfe6e13b0', 'null', '6b0a18cc-376b-4e5c-85ef-df2701014bd3', TO_TIMESTAMP('2022-06-24 05:27:12.000000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), NULL, NULL, '1', '{\"textField\":\"\",\"opinionCoverOrAll\":\"ALL\",\"opinionType\":\"INPUT\",\"Note\":\"edit\",\"printDocForm\":\"1\",\"fanHuiWenShu\":\"1\",\"chuanyue\":\"lingdao\",\"endName\":\"反馈\",\"mishunoedit\":\"1\"}', TO_TIMESTAMP('2022-06-24 14:11:29.441000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), TO_TIMESTAMP('2022-06-24 05:27:47.000000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0', '69e8ab3726da4aa79fa5266acfe4a53d')";
        statement.execute(sql);
    
        sql = "INSERT INTO \"OA_INCOMING_DISPATCHES\"(\"SYS_ID\", \"DOCUMENT_IDENTIFIER\", \"ISSUED_NUMBER_OF_DOCUMENT\", \"DOCUMENT_TYPE\", \"SERIAL_NUMBER_OF_COPIES\", \"EMERGENCY_DEGREE\", \"DOCUMENT_TITLE\", \"ANNOTATION\", \"INSTANCE_ID\", \"SERIAL_NUMBER\", \"DOCUMENT_TIME\", \"IS_ARCHIVED\", \"ARCHIVED_DATE\", \"IS_FINISH\", \"FINISH_DATE\", \"DOCUMENT_WORKFLOW_CODE\", \"DOCUMENT_MAIN_ATTACHMENT_ID\", \"FROM_DEPARTMENT\", \"REGISTER_NAME\", \"PROCESSING_TIME_LIMIT\", \"ATTACHMENT_THICKENING\", \"MODIFY_DATE\", \"CREATE_TIME\", \"ATTACHMENT_NAME\", \"ATTACHMENT_TYPE\", \"FONT_STYLE\", \"YEAR\", \"UNIT\", \"NUMBER_ID\", \"SERIAL_NO\", \"REGISTER_ID\", \"DEPART_TITLE_TYPE\", \"PER_FORM_URL\", \"DOCUMENT_HEADER\", \"FENFA_OR_SIGN\", \"FLOW_SERIAL_NUMBER\", \"BARCODE\", \"CATEGORY_ID\", \"IS_LOCK\", \"LOCK_USER_MESSAGE\", \"RECYCLE_FLAG\", \"DEPART_TITLE_TYPE_ID\", \"INSIDE_STATE\", \"REMARK\", \"IS_DIFFERENTIATE\", \"IS_LINGDAO_TYPE\") VALUES ('9222a5f4146245848d7dcde73537c7d9', '部门收文', '〔2022〕', 'DOCUMENT_INCOMING_02', '1', 'EMERGENCY_DEGREE_04', '测试-公司会议纪要领导秘书短信提醒0615', NULL, 'bb8cef4c-f211-4f19-ac72-a3ba674c4461', NULL, TO_TIMESTAMP('2022-08-21 13:13:07.294000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), NULL, NULL, NULL, NULL, 'BuMenShouWen', 'ff3172f3c176486ab946b504163428a6', '办公厅', '李毓娟', NULL, NULL, NULL, TO_TIMESTAMP('2022-08-21 13:13:07.285000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '6.16升级问题.docx', 'docx', NULL, NULL, NULL, NULL, NULL, 'd284afccb61e4cddbef8de81f70dd1f9', '办公厅', '../../registrationManagement/department/bm_sw.html', NULL, 'fenfa', NULL, 'GB0626-2005^9222a5f4146245848d7dcde73537c7d9^办公厅^DOCUMENT_INCOMING_02^〔2022〕^无^测试-公司会议纪要领导秘书短信提醒0615^无^EMERGENCY_DEGREE_04^2022-08-21 13:13:07.294^无^无^无^无^|', NULL, NULL, NULL, '0', 'dda2c17b1a0d4f238a4a40e17f5bc1f5', NULL, '0', NULL, NULL)";
        statement.execute(sql);
    
        sql = "UPDATE \"OA_WORKITEMS\" SET \"DISPLAY_NAME\" = '秘书FFFFF' WHERE SYS_ID = '493bb8c5444a4ff8863eb7d25bf331f4'";
        statement.execute(sql);
    
        sql = "SELECT * FROM \"OA_WORKITEMS\" WHERE SYS_ID = '493bb8c5444a4ff8863eb7d25bf331f4'";
        statement.execute(sql);
    
        sql = "DELETE FROM \"OA_WORKITEMS\" WHERE SYS_ID = '493bb8c5444a4ff8863eb7d25bf331f4'";
        statement.execute(sql);
    
        sql = "SELECT * FROM \"OA_WORKITEMS\" WHERE SYS_ID = '493bb8c5444a4ff8863eb7d25bf331f4'";
        statement.execute(sql);
        
        sql = "DROP TABLE \"GTCRCOA\".\"OA_INCOMING_DISPATCHES\"";
        statement.execute(sql);
    
        sql = "DROP TABLE \"GTCRCOA\".\"OA_OUTGOING_MESSAGE\"";
        statement.execute(sql);
    
        sql = "DROP TABLE \"GTCRCOA\".\"OA_WORKITEMS\"";
        statement.execute(sql);
    
        statement.close();
    }
    
    private void assertGeneratedKey(final PreparedStatement preparedStatement, final int index) throws SQLException {
        preparedStatement.setInt(1, index);
        preparedStatement.setString(2, "TEST" + index);
        preparedStatement.executeUpdate();
        ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        while (generatedKeys.next()) {
            log.info("generatedKey:{}", generatedKeys.getObject(1));
            assertNotNull(generatedKeys.getMetaData().getColumnLabel(1));
            assertNotNull(generatedKeys.getMetaData().getColumnName(1));
        }
        preparedStatement.close();
    }
    
    @After
    public void cleanUp() throws SQLException {
//        Statement statement = connection.createStatement();
//        statement.execute("DROP TABLE \"GTCRCOA\".\"OA_INCOMING_DISPATCHES\";");
//        statement.close();
        connection.close();
    }
}
