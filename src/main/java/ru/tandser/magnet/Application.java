package ru.tandser.magnet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public class Application {

    private static final String FILE_1 = "1.xml";
    private static final String FILE_2 = "2.xml";

    private static BufferedReader console = new BufferedReader(new InputStreamReader(System.in));

    public static void main(String[] args) throws Exception {
        Core core = new Core();
        System.out.print("url        : ");
        core.setUrl(console.readLine());
        System.out.print("username   : ");
        core.setUsername(console.readLine());
        System.out.print("password   : ");
        core.setPassword(console.readLine());
        System.out.print("n          : ");
        core.setN(Integer.parseInt(console.readLine()));
        console.close();
        try {
            long start = System.nanoTime();
            InputStream stylesheet = Application.class.getResourceAsStream("/entries.xsl");
            core.insert();
            System.out.println("inserting  : successfully");
            core.retrieve(FILE_1);
            System.out.println("retrieving : successfully");
            core.convert(stylesheet, FILE_1, FILE_2);
            System.out.println("converting : successfully");
            BigInteger sum = core.sum(FILE_2);
            core.dispose();
            long duration = System.nanoTime() - start;
            System.out.println("parsing    : successfully");
            System.out.println("sum        : " + sum.toString());
            long minutes = TimeUnit.NANOSECONDS.toMinutes(duration);
            long seconds = TimeUnit.NANOSECONDS.toSeconds(duration);
            System.out.println(String.format("duration   : %d min %d s", minutes, seconds));
        } catch (CoreException ignored) {}
    }
}