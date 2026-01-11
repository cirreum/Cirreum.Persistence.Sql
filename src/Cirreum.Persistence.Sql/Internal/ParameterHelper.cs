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

	public static Dictionary<string, object?> MergeWithPaging(object? parameters, int pageSize, int page) {
		var merged = ToDictionary(parameters);

		// Only set PageSize if not already provided (allows passing request object with PageSize property)
		if (!merged.ContainsKey("PageSize")) {
			merged["PageSize"] = pageSize;
		}

		// Always calculate and set Offset from page/pageSize
		merged["Offset"] = (page - 1) * pageSize;
		return merged;
	}

	public static (Dictionary<string, object?> Merged, int PageSize, int Page) MergeWithPaging(object parameters) {
		var merged = ToDictionary(parameters);

		if (!merged.TryGetValue("PageSize", out var pageSizeObj) || pageSizeObj is not int pageSize) {
			throw new ArgumentException(
				"Parameters must include a 'PageSize' property of type int.",
				nameof(parameters));
		}

		if (!merged.TryGetValue("Page", out var pageObj) || pageObj is not int page) {
			throw new ArgumentException(
				"Parameters must include a 'Page' property of type int.",
				nameof(parameters));
		}

		merged["Offset"] = (page - 1) * pageSize;
		return (merged, pageSize, page);
	}

	public static Dictionary<string, object?> ToDictionary(object? parameters) {
		if (parameters is null) {
			return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
		}

		// If it's already a dictionary with string keys, just wrap/copy it
		if (parameters is IDictionary<string, object?> existingDict) {
			return new Dictionary<string, object?>(existingDict, StringComparer.OrdinalIgnoreCase);
		}

		var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
		foreach (var prop in parameters.GetType().GetProperties()) {
			// Skip indexer properties
			if (prop.GetIndexParameters().Length > 0) {
				continue;
			}
			dict[prop.Name] = prop.GetValue(parameters);
		}

		return dict;
	}

}