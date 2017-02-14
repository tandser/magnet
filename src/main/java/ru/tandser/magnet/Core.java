package ru.tandser.magnet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.sql.*;

public class Core implements Serializable {

    public static final String DELETE = "DELETE FROM test";
    public static final String INSERT = "INSERT INTO test(field) VALUES (?)";
    public static final String SELECT = "SELECT * FROM test";

    private String driverClassName;
    private String url;
    private String username;
    private String password;

    private transient Connection connection;

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

    public void insert(int n) {
        if (n < 0) {
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
            throw new RuntimeException(exc);
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException ignored) {}
            }
        }
    }

    public void retrieve(String output) throws ClassNotFoundException {
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
                transformer.transform(new DOMSource(document), new StreamResult(Paths.get(output).toFile()));
            } catch (SQLException exc) {
                exc.printStackTrace();
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException ignored) {}
                }
            }

        } catch (Exception exc) {
            throw new RuntimeException(exc);
        } finally {
            try {
                connection.close();
            } catch (SQLException ignored) {}
        }
    }

    public void convert(String stylesheet, String input, String output) throws Exception {
        File xslt = Paths.get(stylesheet).toFile();
        Transformer transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(xslt));
        transformer.transform(new StreamSource(input), new StreamResult(Paths.get(output).toFile()));
    }

    public BigInteger sum(String input) {
        File xml = Paths.get(input).toFile();

        BigInteger result = BigInteger.ZERO;

        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xml);
            NodeList nodeList = document.getElementsByTagName("entry");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element entry = (Element) nodeList.item(i);
                result = result.add(new BigInteger(entry.getAttribute("field")));
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        Core core = new Core();
        core.setDriverClassName("org.postgresql.Driver");
        core.setUrl("jdbc:postgresql://localhost:5432/postgres");
        core.setUsername("postgres");
        core.setPassword("postgres");

//        long start = System.currentTimeMillis();

        core.insert(1_000_000);
//        System.out.println("insert");
//        core.retrieve("1.xml");
//        System.out.println("retrieve");
//        core.convert("src/main/resources/entries.xsl", "1.xml", "2.xml");
//        BigInteger sum = core.sum("2.xml");

//        System.out.println((System.currentTimeMillis() - start) + " ms");

//        System.out.println(sum.toString());
    }
}