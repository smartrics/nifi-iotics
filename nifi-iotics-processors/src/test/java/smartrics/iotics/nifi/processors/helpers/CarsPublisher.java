package smartrics.iotics.nifi.processors.helpers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CarsPublisher {

    public static void main(String[] args) throws IOException {
        Path path = Path.of("src/test/resources/twins_more.json");
        String content = Files.readString(path);

        // Define the type of the data we're reading (List<Car>)
        Type carListType = new TypeToken<List<CarModel>>(){}.getType();

        // Create a Gson instance
        Gson gson = new Gson();

        // Deserialize the JSON array to a List<Car>
        List<CarModel> cars = gson.fromJson(content, carListType);

        // Example usage
        for (CarModel car : cars) {
            System.out.println(car);
        }
    }

}
