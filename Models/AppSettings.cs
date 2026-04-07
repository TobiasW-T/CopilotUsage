namespace CopilotUsage.Models;

public sealed class AppSettings
{
	/// <summary>
	/// Optional override for the GitHub OAuth App Client ID.
	/// When empty (the default), the built-in "Copilot Usage" app Client ID is used.
	/// Only set this manually in settings.json if your organisation's GitHub policy
	/// blocks the built-in app and you need to supply your own OAuth App Client ID.
	/// This field is not exposed in the Settings UI.
	/// </summary>
	public string GitHubClientId { get; set; } = string.Empty;

	/// <summary>OAuth access token obtained via GitHub device flow.</summary>
	public string AccessToken { get; set; } = string.Empty;

	/// <summary>Refresh interval in minutes. Valid values: 1, 5, 15, 60.</summary>
	public int RefreshIntervalMinutes { get; set; } = 5;

	public bool StartWithWindows { get; set; } = false;
}
