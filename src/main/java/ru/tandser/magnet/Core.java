package ru.tandser.magnet;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.sql.*;

public class Core implements Serializable {

    public static final String DELETE = "DELETE FROM test";
    public static final String INSERT = "INSERT INTO test(field) VALUES (?)";
    public static final String SELECT = "SELECT * FROM test";

    private String  url;
    private String  username;
    private String  password;
    private Integer n;

    private transient Connection connection;

    public Core() {}

    public Core(String url, String username, String password, Integer n) {
        this.url      = url;
        this.username = username;
        this.password = password;
        this.n        = n;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }

    private void printExceptionMessage(Exception exc) {
        if (exc.getMessage() != null && !exc.getMessage().isEmpty()) {
            System.err.println(exc.getMessage());
        }
    }

    public void insert() throws CoreException {
        if (n == null || n <= 0) {
            throw new IllegalStateException("n = " + n);
        }
        try {
            connection = DriverManager.getConnection(getUrl(), getUsername(), getPassword());
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement();
                    PreparedStatement preparedStatement = connection.prepareStatement(INSERT)) {
                statement.execute(DELETE);
                for (int i = 1; i <= n; i++) {
                    preparedStatement.setInt(1, i);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception exc1) {
                printExceptionMessage(exc1);
                try {
                    connection.rollback();
                } catch (Exception exc2) {
                    printExceptionMessage(exc2);
                }
                throw new CoreException();
            }
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                } catch (Exception exc) {
                    printExceptionMessage(exc);
                }
            }
        }
    }

    public void retrieve(String filename) throws CoreException {
        try {
            connection.setReadOnly(true);
            try (Statement statement = connection.createStatement();
                    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(filename))) {
                ResultSet resultSet = statement.executeQuery(SELECT);
                XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance()
                        .createXMLStreamWriter(outputStream, "UTF-8");
                xmlStreamWriter.writeStartDocument("UTF-8", "1.0");
                xmlStreamWriter.writeStartElement("entries");
                while (resultSet.next()) {
                    xmlStreamWriter.writeStartElement("entry");
                    xmlStreamWriter.writeStartElement("field");
                    xmlStreamWriter.writeCharacters(resultSet.getString(1));
                    xmlStreamWriter.writeEndElement();
                    xmlStreamWriter.writeEndElement();
                }
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeEndDocument();
                xmlStreamWriter.flush();
                xmlStreamWriter.close();
            } catch (Exception exc) {
                printExceptionMessage(exc);
                throw new CoreException();
            }
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        } finally {
            if (connection != null) {
                try {
                    connection.setReadOnly(false);
                } catch (Exception exc) {
                    printExceptionMessage(exc);
                }
            }
        }
    }

    public void convert(InputStream stylesheet, String input, String output) throws CoreException {
        try {
            Transformer transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(stylesheet));
            transformer.transform(new StreamSource(input), new StreamResult(Paths.get(output).toFile()));
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        }
    }

    public BigInteger parse(String filename) throws CoreException {
        try {
            return new DefaultHandler() {
                private BigInteger accumulator = BigInteger.ZERO;

                public BigInteger sum() throws Exception {
                    SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
                    saxParser.parse(new BufferedInputStream(new FileInputStream(filename)), this);
                    return accumulator;
                }

                @Override
                public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                    if (qName.equals("entry")) {
                        accumulator = accumulator.add(new BigInteger(attributes.getValue("field")));
                    }
                }
            }.sum();
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        }
    }

    public void dispose() throws CoreException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        }
    }
}