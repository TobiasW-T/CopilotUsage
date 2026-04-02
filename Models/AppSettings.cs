namespace CopilotUsage.Models;

public sealed class AppSettings
{
	/// <summary>
	/// GitHub OAuth App Client ID used for the device authorization flow.
	/// Register your own app at https://github.com/settings/applications/new
	/// (enable Device Flow, set any callback URL) to show your app's name on
	/// the GitHub authorization page instead of "GitHub CLI".
	/// Leave empty to fall back to the built-in client ID.
	/// </summary>
	public string GitHubClientId { get; set; } = string.Empty;

	/// <summary>OAuth access token obtained via GitHub device flow.</summary>
	public string AccessToken { get; set; } = string.Empty;

	/// <summary>Refresh interval in minutes. Valid values: 1, 5, 15, 60.</summary>
	public int RefreshIntervalMinutes { get; set; } = 15;

	public bool StartWithWindows { get; set; } = false;
}
