package ru.tandser.magnet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.sql.*;

import static java.lang.String.format;

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
            System.out.println(format("exception: %s", exc.getMessage()));
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

    public void retrieve(String output) throws CoreException {
        try {
            connection.setReadOnly(true);
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(SELECT);
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
                Element entries = document.createElement("entries");
                while (resultSet.next()) {
                    Element entry = document.createElement("entry");
                    Element field = document.createElement("field");
                    field.appendChild(document.createTextNode(Integer.toString(resultSet.getInt(1))));
                    entry.appendChild(field);
                    entries.appendChild(entry);
                }
                document.appendChild(entries);
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.transform(new DOMSource(document), new StreamResult(Paths.get(output).toFile()));
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

    public BigInteger sum(String input) throws CoreException {
        try {
            File source = Paths.get(input).toFile();
            BigInteger sum = BigInteger.ZERO;
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(source);
            NodeList nodeList = document.getElementsByTagName("entry");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element entry = (Element) nodeList.item(i);
                sum = sum.add(new BigInteger(entry.getAttribute("field")));
            }
            return sum;
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