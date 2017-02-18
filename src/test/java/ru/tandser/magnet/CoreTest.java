package ru.tandser.magnet;

import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class CoreTest {

    private static final String FILE_1 = "test1.xml";
    private static final String FILE_2 = "test2.xml";

    private static final String  URL      = "jdbc:hsqldb:mem:hsqldb";
    private static final String  USERNAME = "sa";
    private static final String  PASSWORD = "";
    private static final Integer N        = 100;

    private static DocumentBuilder documentBuilder;
    private static Connection      connection;
    private static Core            core;
    private static BigInteger      sum;

    @BeforeClass
    public static void beforeClass() throws Exception {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        documentBuilderFactory.setCoalescing(true);
        documentBuilderFactory.setIgnoringElementContentWhitespace(true);
        documentBuilderFactory.setIgnoringComments(true);
        documentBuilder = documentBuilderFactory.newDocumentBuilder();
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE test (field INTEGER NOT NULL);");
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(Core.INSERT)) {
            for (int i = -10; i <= 0; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
        core = new Core(URL, USERNAME, PASSWORD, N);
        sum = Stream.iterate(BigInteger.ONE, n -> n.add(BigInteger.ONE)).limit(N).reduce(BigInteger::add).orElse(BigInteger.ZERO);
    }

    @Test
    public void testInsert() throws Exception {
        core.insert();
        core.dispose();
        BigInteger actual = BigInteger.ZERO;
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(Core.SELECT);
            while (resultSet.next()) {
                actual = actual.add(BigInteger.valueOf(resultSet.getInt(1)));
            }
        }
        assertTrue(actual.equals(sum));
    }

    @Test
    public void testRetrieve() throws Exception {
        core.insert();
        core.retrieve(FILE_1);
        core.dispose();
        Document actual = parse(Paths.get(FILE_1).toFile());
        validator("/schema1.xsd").validate(new DOMSource(actual));
        Document extended = parse(getResource("/mock1.xml"));
        assertTrue(actual.isEqualNode(extended));
    }

    @Test
    public void testConvert() throws Exception {
        core.insert();
        core.retrieve(FILE_1);
        try (InputStream stylesheet = getClass().getResourceAsStream("/entries.xsl")) {
            core.convert(stylesheet, FILE_1, FILE_2);
        }
        core.dispose();
        Document actual = parse(Paths.get(FILE_2).toFile());
        validator("/schema2.xsd").validate(new DOMSource(actual));
        Document extended = parse(getResource("/mock2.xml"));
        assertTrue(actual.isEqualNode(extended));
    }

    @Test
    public void testSum() throws Exception {
        core.insert();
        core.retrieve(FILE_1);
        try (InputStream stylesheet = getClass().getResourceAsStream("/entries.xsl")) {
            core.convert(stylesheet, FILE_1, FILE_2);
        }
        BigInteger actual = core.parse(FILE_2);
        core.dispose();
        assertTrue(actual.equals(sum));
    }

    private static Validator validator(String filename) throws Exception {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = schemaFactory.newSchema(getResource(filename));
        return schema.newValidator();
    }

    private static Document parse(File source) throws Exception {
        Document document = documentBuilder.parse(source);
        document.normalizeDocument();
        return document;
    }

    private static File getResource(String filename) throws Exception {
        URI uri = CoreTest.class.getResource(filename).toURI();
        return Paths.get(uri).toFile();
    }
}