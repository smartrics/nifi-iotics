package smartrics.iotics.nifi.processors.helpers;

import com.google.gson.annotations.SerializedName;

public record CarModel(String ID, String Make, String Model, String Colour, boolean isOperational, String Comment,
                       int Units, @SerializedName("class") String classUrl, String label) {
}