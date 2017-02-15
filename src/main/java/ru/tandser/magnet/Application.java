package ru.tandser.magnet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class Application {

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
                print("exception: Fatal error. The application will be closed");
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

                    if (result <= 0) {
                        println(error);
                        print(message);
                        continue;
                    }

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

        Console.print("url: ");
        core.setUrl(Console.read());

        Console.print("username: ");
        core.setUsername(Console.read());

        Console.print("password: ");
        core.setPassword(Console.read());

        Integer n = Console.read("N: ", "error: Incorrect input. Try again or enter \"exit\" to interrupt");

        Console.close();

        if (n == null) {
            System.exit(0);
        }

        URI stylesheet = Application.class.getResource("/entries.xsl").toURI();

        try {
            long start = System.nanoTime();

            core.insert(n);
            Console.println("inserting: successfully");

            core.retrieve(FILE_1);
            Console.println("retrieving: successfully");

            core.convert(stylesheet, FILE_1, FILE_2);
            Console.println("converting: successfully");

            BigInteger sum = core.sum(FILE_2);

            long duration = System.nanoTime() - start;

            Console.println("parsing: successfully");
            Console.println("sum: " + sum.toString());

            long minutes = TimeUnit.NANOSECONDS.toMinutes(duration);
            long seconds = TimeUnit.NANOSECONDS.toSeconds(duration);

            Console.print(String.format("duration: %d min %d s", minutes, seconds));

            core.dispose();
        } catch (CoreException ignored) {}
    }
}