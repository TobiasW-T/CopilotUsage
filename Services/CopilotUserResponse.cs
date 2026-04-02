using System.Text.Json.Serialization;

namespace CopilotUsage.Services;

internal sealed class CopilotUserResponse
{
	[JsonPropertyName( "quota_reset_date" )]
	public string? QuotaResetDate { get; set; }

	[JsonPropertyName( "quota_snapshots" )]
	public QuotaSnapshots? QuotaSnapshots { get; set; }
}
