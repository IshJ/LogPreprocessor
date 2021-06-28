import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LogPreprocessor {
    static final String logPath = "/home/ishadi/Desktop/log3.out";
    static final String dbHome = "/home/ishadi/Documents/AndroidCFI/apps/weather/v1/db/";
    static final String MainAppDbPath = dbHome + "ground_truth_full.out";
    static final String sideChannelDbPath = dbHome + "side_channel_info_full.out";
    static final String MainAppProcessedPath = dbHome + "ground_truth.out";
    static final String sideChannelProcessedPath = dbHome + "side_channel_info.out";
    static final String sideChannelProcessedPath1 = dbHome + "side_channel_info_1.out";
    static final String sideChannelProcessedPath2 = dbHome + "side_channel_info_2.out";
    static final String sideChannelProcessedPath3 = dbHome + "side_channel_info_3.out";
    static final String sideChannelProcessedPath4 = dbHome + "side_channel_info_4.out";
    static final String sideChannelProcessedPath5 = dbHome + "side_channel_info_5.out";

    public static void main(String[] args) throws IOException {
        List<String> logLines = Files.lines(Paths.get(logPath), StandardCharsets.ISO_8859_1).collect(Collectors.toList());
//            List<String> groundTruthLines = Files.lines(Paths.get(dbHome + "ground_truth.out"), StandardCharsets.ISO_8859_1).collect(Collectors.toList());
//            List<String> sideChannelLines = Files.lines(Paths.get(dbHome + "side_channel_info.out"), StandardCharsets.ISO_8859_1).collect(Collectors.toList());
        List<String> groundTruthLines = Files.lines(Paths.get(MainAppDbPath), StandardCharsets.ISO_8859_1).collect(Collectors.toList());
        List<String> sideChannelLines = Files.lines(Paths.get(sideChannelDbPath), StandardCharsets.ISO_8859_1).collect(Collectors.toList());

        sideChannelLines = preprocessData(groundTruthLines, sideChannelLines);
        int fileLimes = sideChannelLines.size() / 5;
        Files.write(Path.of(sideChannelProcessedPath1), sideChannelLines.subList(0, fileLimes), StandardCharsets.ISO_8859_1);
        Files.write(Path.of(sideChannelProcessedPath2), sideChannelLines.subList(fileLimes, fileLimes * 2), StandardCharsets.ISO_8859_1);
        Files.write(Path.of(sideChannelProcessedPath3), sideChannelLines.subList(fileLimes * 2, fileLimes * 3), StandardCharsets.ISO_8859_1);
        Files.write(Path.of(sideChannelProcessedPath4), sideChannelLines.subList(fileLimes * 3, fileLimes * 4), StandardCharsets.ISO_8859_1);
        Files.write(Path.of(sideChannelProcessedPath5), sideChannelLines.subList(fileLimes * 4, sideChannelLines.size()), StandardCharsets.ISO_8859_1);

        Files.write(Path.of(sideChannelProcessedPath), sideChannelLines, StandardCharsets.ISO_8859_1);
        Files.write(Path.of(MainAppProcessedPath), groundTruthLines, StandardCharsets.ISO_8859_1);
    }

    private static List<String> preprocessData(List<String> groundTruthLines, List<String> sideChannelLines) {
        Long maxTIme = groundTruthLines.stream().mapToLong(l -> Long.parseLong(l.split("\\|")[0])).max().getAsLong() + 1;
        Long minTIme = groundTruthLines.stream().mapToLong(l -> Long.parseLong(l.split("\\|")[0])).min().getAsLong() - 1;

        List<String> cappedSideChannelLines = sideChannelLines.stream()
                .filter(s -> minTIme < Long.parseLong(s.split("\\|")[0]) && Long.parseLong(s.split("\\|")[0]) < maxTIme)
                .collect(Collectors.toList());


        Map<Long, String> countsMap = cappedSideChannelLines.parallelStream()
                .collect(Collectors.toMap(s -> Long.parseLong(s.split("\\|")[0]), s -> s.split("\\|")[3], (s1, s2) -> s1 + "," + s2));
        Map<Long, List<Long>> countListMap = new HashMap<>();


        List<Long> counts = countsMap.keySet().stream().sorted().collect(Collectors.toList());
        counts.forEach(i -> countListMap.put(i, new ArrayList<>()));
        countsMap.keySet().forEach(i -> countListMap.get(i).addAll(Arrays.stream(countsMap.get(i).split(",")).collect(Collectors.toList())
                .stream().map(Long::valueOf).collect(Collectors.toList())));

        List<Long> groundTruthCounts = new ArrayList<>();

        for (int i = 0; i < groundTruthLines.size(); i++) {
            String groundTruthLine = groundTruthLines.get(i);
            if (groundTruthLine.contains("-1")) {
                Long timing = Long.parseLong(groundTruthLine.split("\\|")[0]);
                if (!counts.contains(timing)) {
                    continue;
                }
                List<Long> matchingCounts = countListMap.get(timing);
                Optional<Long> matchingCount = matchingCounts.stream().filter(c -> !groundTruthCounts.contains(c)).findFirst();
                if (matchingCount.isPresent()) {
                    groundTruthCounts.add(matchingCount.get());
                    groundTruthLine = groundTruthLine.replace("-1", matchingCount.get().toString());
                    groundTruthLines.set(i, groundTruthLine);
                }


            }
        }
        List<String> tempList = new ArrayList<>(groundTruthLines);
        tempList.stream().filter(s -> s.contains("-1")).forEach(groundTruthLines::remove);

//        Long minCount = groundTruthLines.stream().mapToLong(s -> Long.parseLong(s.split("\\|")[2])).min().getAsLong() - 1l;
//        Long maxCount = groundTruthLines.stream().mapToLong(s -> Long.parseLong(s.split("\\|")[2])).max().getAsLong() + 1l;
//
//        tempList = new ArrayList<>(cappedSideChannelLines);
//        tempList.stream().filter(s -> {
//            Long count = Long.parseLong(s.split("\\|")[3]);
//            return minCount > count || count > maxCount;
//        }).forEach(s -> cappedSideChannelLines.remove(s));


        return cappedSideChannelLines;
    }
}
