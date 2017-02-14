package ru.tandser.magnet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.Serializable;
import java.io.StringWriter;
import java.sql.*;

public class Core implements Serializable {

    public static final String DELETE = "DELETE FROM test";
    public static final String INSERT = "INSERT INTO test(field) VALUES (?)";
    public static final String SELECT = "SELECT * FROM test";

    private String driverClassName;
    private String url;
    private String username;
    private String password;

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
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

    public void insert(int n) throws ClassNotFoundException {
        if (n < 0) {
            throw new IllegalArgumentException();
        }

        Class.forName(getDriverClassName());

        try (Connection connection = DriverManager.getConnection(getUrl(), getUsername(), getPassword())) {

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
            } catch (SQLException exc) {
                try {
                    connection.rollback();
                } catch (SQLException ignored) {}
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException ignored) {}
                }

                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException ignored) {}
                }
            }
        } catch (Exception exc) {

        }
    }

    public void retrieve() throws ClassNotFoundException {
        Class.forName(getDriverClassName());

        try (Connection connection = DriverManager.getConnection(getUrl(), getUsername(), getPassword())) {

            Statement statement = null;

            try {
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(SELECT);

                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
                Element entries = document.createElement("entries");
                while (resultSet.next()) {
                    Element entry = document.createElement("entry");
                    Element field = document.createElement("field");
                    field.appendChild(document.createTextNode(Integer.toString(resultSet.getInt("field"))));
                    entry.appendChild(field);
                    entries.appendChild(entry);
                }
                document.appendChild(entries);

                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

                DOMSource domSource = new DOMSource(document);
                StreamResult streamResult = new StreamResult(new File("1.xml"));
                transformer.transform(domSource, streamResult);
            } catch (SQLException exc) {

            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException ignored) {}
                }
            }

        } catch (Exception exc) {

        }
    }

    public static void main(String[] args) throws Exception {
        Core core = new Core();
        core.setDriverClassName("org.postgresql.Driver");
        core.setUrl("jdbc:postgresql://localhost:5432/postgres");
        core.setUsername("postgres");
        core.setPassword("postgres");
        core.insert(100);
        core.retrieve();
    }
}