package ru.tandser.magnet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.String.format;

public class Application {

    private static String pattern1 = "%-11.11s : ";
    private static String pattern2 = "%-11.11s : %s";

    private static final String FILE_1 = "1.xml";
    private static final String FILE_2 = "2.xml";

    private static class Console {
        private static BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        public static void print(String message) {
            System.out.print(message);
        }

        public static void println(String message) {
            System.out.println(message);
        }

        public static String read() {
            String input = null;

            try {
                input = reader.readLine();
            } catch (Exception exc) {
                print("Fatal error. The application will be closed");
                System.exit(1);
            }

            return input;
        }

        public static Integer read(String message, String error) {
            String input;
            Integer result = null;

            print(message);

            while (!"exit".equalsIgnoreCase(input = read())) {
                try {
                    result = Integer.valueOf(input);
                    break;
                } catch (Exception exc) {
                    println(error);
                    print(message);
                }
            }

            return result;
        }

        public static void close() {
            try {
                reader.close();
            } catch (Exception ignored) {}
        }
    }

    public static void main(String[] args) throws URISyntaxException {
        Core core = new Core();

        Console.print(format(pattern1, "url"));
        core.setUrl(Console.read());

        Console.print(format(pattern1, "username"));
        core.setUsername(Console.read());

        Console.print(format(pattern1, "password"));
        core.setPassword(Console.read());

        Integer n = Console.read(format(pattern1, "N"), format(pattern2, "error", "Incorrect input. Try again or enter \"exit\" to interrupt"));

        Console.close();

        if (n == null) {
            System.exit(0);
        }

        URI stylesheet = core.getClass().getResource("/entries.xsl").toURI();

        try {
            long start = System.currentTimeMillis();

            core.insert(n);
            Console.println(format(pattern2, "inserting", "successfull"));

            core.retrieve(FILE_1);
            Console.println(format(pattern2, "retrieving", "successfull"));

            core.convert(stylesheet, FILE_1, FILE_2);
            Console.println(format(pattern2, "converting", "successfull"));

            BigInteger sum = core.sum(FILE_2);

            String duration = Long.toString(System.currentTimeMillis() - start) + " ms";

            Console.println(format(pattern2, "parsing", "successfull"));
            Console.println(format(pattern2, "sum", sum.toString()));
            Console.print(format(pattern2, "duration", duration));

            core.dispose();
        } catch (CoreException ignored) {}
    }
}