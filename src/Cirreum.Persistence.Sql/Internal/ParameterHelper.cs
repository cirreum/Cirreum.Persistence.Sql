namespace Cirreum.Persistence.Internal;

internal static class ParameterHelper {

	public static Dictionary<string, object?> MergeWithPageSize(object? parameters, int pageSize) {
		var merged = ToDictionary(parameters);

		if (merged.ContainsKey("PageSize")) {
			throw new ArgumentException(
				"Parameters must not include 'PageSize'; it is added automatically from the pageSize parameter.",
				nameof(parameters));
		}

		merged["PageSize"] = pageSize;
		return merged;
	}

	public static Dictionary<string, object?> ToDictionary(object? parameters) {
		var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

		if (parameters is null) {
			return dict;
		}

		foreach (var prop in parameters.GetType().GetProperties()) {
			dict[prop.Name] = prop.GetValue(parameters);
		}

		return dict;
	}

}