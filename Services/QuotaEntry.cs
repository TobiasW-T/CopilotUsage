using System.Text.Json.Serialization;

namespace CopilotUsage.Services;

internal sealed class QuotaEntry
{
	[JsonPropertyName( "entitlement" )]
	public int Entitlement { get; set; }

	[JsonPropertyName( "remaining" )]
	public int Remaining { get; set; }

	[JsonPropertyName( "unlimited" )]
	public bool Unlimited { get; set; }

	[JsonPropertyName( "overage_count" )]
	public int OverageCount { get; set; }

	[JsonPropertyName( "overage_permitted" )]
	public bool OveragePermitted { get; set; }

	[JsonPropertyName( "percent_remaining" )]
	public double PercentRemaining { get; set; }
}
