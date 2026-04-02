using System.Text.Json.Serialization;

namespace CopilotUsage.Services;

internal sealed class QuotaSnapshots
{
	[JsonPropertyName( "premium_interactions" )]
	public QuotaEntry? PremiumInteractions { get; set; }
}
