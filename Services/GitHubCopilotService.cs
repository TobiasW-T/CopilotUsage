using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using CopilotUsage.Models;

namespace CopilotUsage.Services;

internal sealed class GitHubCopilotService
{
	// Same endpoint used by the VS Code Copilot Chat extension (microsoft/vscode-copilot-chat).
	// Auth scheme is "token" (not "Bearer"), API version is 2025-04-01.
	private const string UserInfoEndpoint = "https://api.github.com/copilot_internal/user";
	private const string ApiVersion = "2025-04-01";

	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

	private readonly HttpClient m_HttpClient;


	public GitHubCopilotService()
	{
		m_HttpClient = new HttpClient();
		m_HttpClient.DefaultRequestHeaders.UserAgent.Add(
			new ProductInfoHeaderValue( "CopilotUsageTray", "1.0" ) );
		m_HttpClient.DefaultRequestHeaders.Accept.Add(
			new MediaTypeWithQualityHeaderValue( "application/json" ) );
		m_HttpClient.DefaultRequestHeaders.Add( "X-GitHub-Api-Version", ApiVersion );
	}


	public async Task<CopilotUsageData> GetUsageAsync( string accessToken )
	{
		if ( string.IsNullOrWhiteSpace( accessToken ) )
		{
			throw new InvalidOperationException( "No access token configured. Please authorize via GitHub first." );
		}

		using var request = new HttpRequestMessage( HttpMethod.Get, UserInfoEndpoint );
		request.Headers.Authorization = new AuthenticationHeaderValue( "token", accessToken );

		var response = await m_HttpClient.SendAsync( request ).ConfigureAwait( false );
		var body = await response.Content.ReadAsStringAsync().ConfigureAwait( false );

		switch ( response.StatusCode )
		{
			case System.Net.HttpStatusCode.OK:
				break;
			case System.Net.HttpStatusCode.Unauthorized:
				throw new InvalidOperationException( "GitHub token is invalid or expired (401). Please re-authorize in Settings." );
			case System.Net.HttpStatusCode.Forbidden:
				throw new InvalidOperationException( $"Access denied (403). Ensure your OAuth App has the required scopes.\n\nResponse: {TrimBody( body )}" );
			case System.Net.HttpStatusCode.NotFound:
				throw new InvalidOperationException( $"Copilot user info endpoint not found (404).\n\nResponse: {TrimBody( body )}" );
			default:
				throw new InvalidOperationException( $"Unexpected response (HTTP {(int) response.StatusCode}).\n\nResponse: {TrimBody( body )}" );
		}

		var user = JsonSerializer.Deserialize<CopilotUserResponse>( body, JsonOptions )
			?? throw new InvalidOperationException( $"Failed to deserialize Copilot user info.\n\nResponse: {TrimBody( body )}" );

		return BuildUsageData( user );
	}


	private static CopilotUsageData BuildUsageData( CopilotUserResponse user )
	{
		var premium = user.QuotaSnapshots?.PremiumInteractions;
		if ( premium == null )
		{
			return new CopilotUsageData { IsUnavailable = true };
		}

		DateTimeOffset? resetAt = null;
		if ( !string.IsNullOrWhiteSpace( user.QuotaResetDate )
			&& DateTimeOffset.TryParse( user.QuotaResetDate, CultureInfo.InvariantCulture, out var parsed ) )
		{
			resetAt = parsed;
		}

		return new CopilotUsageData
		{
			Limit = premium.Entitlement,
			Used = premium.Entitlement - premium.Remaining,
			Unlimited = premium.Unlimited,
			ResetAt = resetAt,
		};
	}


	private static string TrimBody( string body ) =>
		body.Length > 300 ? body[..300] + "…" : body;
}
