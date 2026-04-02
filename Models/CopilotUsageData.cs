using System.Text.Json.Serialization;

namespace CopilotUsage.Models;

/// <summary>
/// Parsed quota data returned by the app for display purposes.
/// Derived from the raw <c>CopilotUserResponse</c> API payload.
/// </summary>
public sealed class CopilotUsageData
{
	/// <summary>Total premium requests allocated for the period.</summary>
	public int Limit { get; set; }

	/// <summary>Premium requests consumed so far (= Limit - Remaining).</summary>
	public int Used { get; set; }

	/// <summary>Whether this subscription has unlimited premium requests.</summary>
	public bool Unlimited { get; set; }

	/// <summary>Date at which the quota resets.</summary>
	public DateTimeOffset? ResetAt { get; set; }

	/// <summary>
	/// Set to <c>true</c> when quota data is simply not present in the API response
	/// (e.g. the account has no premium_interactions snapshot). The UI shows "N/A".
	/// </summary>
	[JsonIgnore]
	public bool IsUnavailable { get; set; }
}