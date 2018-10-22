package gov.usgs.aqcu;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONArrayAs;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

import gov.usgs.aqcu.util.AqcuGsonBuilderFactory;

public final class ObjectCompare {

	private ObjectCompare() {}

	public static void compare(final Object expectedObject, final Object actualObject) {
		Gson gson = AqcuGsonBuilderFactory.getConfiguredGsonBuilder()
				.serializeNulls()
				.create();
		try {
			assertThat(new JSONObject(gson.toJson(actualObject)),
					sameJSONObjectAs(new JSONObject(gson.toJson(expectedObject))).allowingAnyArrayOrdering());
		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}

	public static void compare(final List<?> expectedObject, final List<?> actualObject) {
		Gson gson = AqcuGsonBuilderFactory.getConfiguredGsonBuilder()
				.serializeNulls()
				.create();
		try {
			assertThat(new JSONArray(gson.toJson(actualObject)),
					sameJSONArrayAs(new JSONArray(gson.toJson(expectedObject))));
		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}
	}
}
