import java.util.*;

/**
 * @author Atom
 */
public class MapTest {

    public static void main(String[] args) {

        List<String> first = new ArrayList<>();
        first.add("id");
        first.add("name");
        List<String> second = new ArrayList<>();
        second.add("myId");
        second.add("address");

        Map<String, String> map = new TreeMap<>();

        for (int i = 0; i < first.size(); i++) {
            map.put(first.get(i), second.get(i));
        }

        System.err.println(map);


    }
}
