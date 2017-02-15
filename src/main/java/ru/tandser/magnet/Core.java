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
import java.io.Serializable;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;

public class Core implements Serializable {

    public static final String DELETE = "DELETE FROM test";
    public static final String INSERT = "INSERT INTO test(field) VALUES (?)";
    public static final String SELECT = "SELECT * FROM test";

    private String url;
    private String username;
    private String password;

    private transient Connection connection;

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

    private void printExceptionMessage(Exception exc) {
        if (exc.getMessage() != null && !exc.getMessage().isEmpty()) {
            System.out.println(String.format("exception: %s", exc.getMessage()));
        }
    }

    private void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception exc) {
                printExceptionMessage(exc);
            }
        }
    }

    public void insert(int n) throws CoreException {
        if (n <= 0) {
            throw new IllegalArgumentException("Argument must be greater than zero");
        }

        try {
            connection = DriverManager.getConnection(getUrl(), getUsername(), getPassword());
            connection.setAutoCommit(false);

            Statement statement = null;
            PreparedStatement preparedStatement = null;

            try {
                statement = connection.createStatement();
                statement.execute(DELETE);

                preparedStatement = connection.prepareStatement(INSERT);
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
            } finally {
                close(preparedStatement);
                close(statement);
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

            Statement statement = null;

            try {
                statement = connection.createStatement();
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
            } finally {
                close(statement);
            }
        } catch (Exception exc) {
            printExceptionMessage(exc);
            throw new CoreException();
        }
    }

    public void convert(URI stylesheet, String input, String output) throws CoreException {
        try {
            File xslt = Paths.get(stylesheet).toFile();
            Transformer transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(xslt));
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