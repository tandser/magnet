package ru.tandser.magnet;

import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class CoreTest {

    private static final int N = 100;

    private static final String FILE_1 = "1_test.xml";
    private static final String FILE_2 = "2_test.xml";

    private static Connection connection;
    private static Core core;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:hsqldb", "sa", "");

        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE test (field INTEGER NOT NULL);");

        statement.close();

        PreparedStatement preparedStatement = connection.prepareStatement(Core.INSERT);
        for (int i = -10; i <= 0; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

        preparedStatement.close();

        core = new Core();
        core.setUrl("jdbc:hsqldb:mem:hsqldb");
        core.setUsername("sa");
        core.setPassword("");
    }

    @Test
    public void testInsert() throws Exception {
        core.insert(N);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(Core.SELECT);

        BigInteger actual = BigInteger.ZERO;
        while (resultSet.next()) {
            actual = actual.add(BigInteger.valueOf(resultSet.getInt(1)));
        }

        statement.close();

        BigInteger expected = Stream.iterate(BigInteger.ONE, n -> n.add(BigInteger.ONE)).limit(N).reduce(BigInteger::add).get();

        assertTrue(actual.equals(expected));
    }

    @Test
    public void testRetrieve() throws Exception {
        core.insert(100);
        core.retrieve(FILE_1);

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        documentBuilderFactory.setCoalescing(true);
        documentBuilderFactory.setIgnoringElementContentWhitespace(true);
        documentBuilderFactory.setIgnoringComments(true);
        Document actual = documentBuilderFactory.newDocumentBuilder().parse(FILE_1);

        URI xsd = core.getClass().getResource("/schema.xsd").toURI();
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = schemaFactory.newSchema(Paths.get(xsd).toFile());
        Validator validator = schema.newValidator();
        validator.validate(new DOMSource(actual));


    }
}