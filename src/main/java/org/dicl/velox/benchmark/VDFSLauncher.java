package org.dicl.velox.benchmark;

import org.dicl.velox.benchmark.ExampleDriver;
import java.lang.ProcessBuilder;
import java.lang.ProcessBuilder.Redirect;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Arrays;
import java.util.List;

public class VDFSLauncher {

    private static void launchCommand(String cmd) {
        try {
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
            pb.inheritIO();
            pb.redirectOutput(Redirect.INHERIT);
            pb.start().waitFor();
        } catch (IOException e) {

        } catch (InterruptedException e) {

        }
    }

    private static String prepareHeader(String className, String inputFile, String args) {
        String jarLocation = "build/libs/velox-hadoop.jar";

        String cmd = " hadoop jar ";
        cmd += jarLocation + " ";
        cmd += className;
        cmd += " -D velox.recordreader.buffersize=838860";
        cmd += " -D mapreduce.map.speculative=false";
        cmd += " -D velox.inputfile=" + inputFile;
        cmd += " -libjars " + jarLocation + " ";
        cmd += args;

        System.out.println(cmd);
        return cmd;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("ERR: Minimun args is 2. Execute this program as: gradle run className Inputfile [ARGS]");
            System.exit(1);
        }

        String className = args[0];
        String inputFile = args[1];
        String cmdArgs = "";

        if (args.length > 1) {
            List<String> remainingArgs = Arrays.asList(args).subList(2, args.length);
            cmdArgs = String.join(" ", remainingArgs);
        }

        launchCommand(prepareHeader(className, inputFile, cmdArgs));
    }
}
