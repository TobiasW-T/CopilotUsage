using System.Text.Json.Serialization;

namespace CopilotUsage.Services;

internal sealed class DeviceCodeResponse
{
	[JsonPropertyName( "device_code" )]
	public string DeviceCode { get; set; } = string.Empty;

	[JsonPropertyName( "user_code" )]
	public string UserCode { get; set; } = string.Empty;

	[JsonPropertyName( "verification_uri" )]
	public string VerificationUri { get; set; } = "https://github.com/login/device";

	[JsonPropertyName( "expires_in" )]
	public int ExpiresIn { get; set; }

	[JsonPropertyName( "interval" )]
	public int Interval { get; set; } = 5;
}
