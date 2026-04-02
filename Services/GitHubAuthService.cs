using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;

namespace CopilotUsage.Services;

/// <summary>
/// Implements the GitHub OAuth Device Flow.
/// Register your own OAuth App at https://github.com/settings/applications/new
/// and enable "Device Flow".
/// </summary>
internal sealed class GitHubAuthService
{
	private const string DeviceCodeUrl = "https://github.com/login/device/code";
	private const string TokenUrl = "https://github.com/login/oauth/access_token";
	private const string GrantType = "urn:ietf:params:oauth:grant-type:device_code";
	private const string Scope = "read:user copilot";

	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

	private readonly HttpClient m_HttpClient;
	private readonly string m_ClientId;


	public GitHubAuthService( string? clientId )
	{
		m_ClientId = clientId?.Trim() ?? string.Empty;
		m_HttpClient = new HttpClient();
		m_HttpClient.DefaultRequestHeaders.Accept.Add(
			new MediaTypeWithQualityHeaderValue( "application/json" ) );
		m_HttpClient.DefaultRequestHeaders.UserAgent.Add(
			new ProductInfoHeaderValue( "CopilotUsageTray", "1.0" ) );
	}


	public async Task<DeviceCodeResponse> RequestDeviceCodeAsync( CancellationToken ct = default )
	{
		if ( string.IsNullOrWhiteSpace( m_ClientId ) )
		{
			throw new InvalidOperationException(
				"No GitHub OAuth App Client ID configured.\n\n" +
				"Register an OAuth App at github.com/settings/applications/new, enable 'Device Flow', " +
				"and paste the Client ID in Settings → Advanced." );
		}

		var content = new FormUrlEncodedContent( new Dictionary<string, string>
		{
			["client_id"] = m_ClientId,
			["scope"] = Scope,
		} );

		var response = await m_HttpClient.PostAsync( DeviceCodeUrl, content, ct ).ConfigureAwait( false );
		var json = await response.Content.ReadAsStringAsync( ct ).ConfigureAwait( false );

		if ( !response.IsSuccessStatusCode )
		{
			throw new InvalidOperationException( ExtractError( json, response.StatusCode.ToString() ) );
		}

		var result = JsonSerializer.Deserialize<DeviceCodeResponse>( json, JsonOptions );
		if ( result == null || string.IsNullOrEmpty( result.DeviceCode ) )
		{
			throw new InvalidOperationException( "GitHub returned an unexpected response. Ensure 'Device Flow' is enabled for your OAuth App." );
		}

		return result;
	}


	public async Task<string> PollForTokenAsync(
		string deviceCode,
		int pollIntervalSeconds,
		Action<string>? onStatus = null,
		CancellationToken ct = default )
	{
		while ( !ct.IsCancellationRequested )
		{
			await Task.Delay( TimeSpan.FromSeconds( pollIntervalSeconds ), ct ).ConfigureAwait( false );

			var content = new FormUrlEncodedContent( new Dictionary<string, string>
			{
				["client_id"] = m_ClientId,
				["device_code"] = deviceCode,
				["grant_type"] = GrantType,
			} );

			var response = await m_HttpClient.PostAsync( TokenUrl, content, ct ).ConfigureAwait( false );
			var json = await response.Content.ReadAsStringAsync( ct ).ConfigureAwait( false );

			if ( !response.IsSuccessStatusCode )
			{
				throw new InvalidOperationException( ExtractError( json, response.StatusCode.ToString() ) );
			}

			var poll = JsonSerializer.Deserialize<TokenPollResponse>( json, JsonOptions )
				?? throw new InvalidOperationException( "Failed to deserialize token poll response." );

			switch ( poll.Error )
			{
				case null or "":
					if ( !string.IsNullOrEmpty( poll.AccessToken ) )
					{
						return poll.AccessToken;
					}

					break;
				case "authorization_pending":
					onStatus?.Invoke( "Waiting for authorization in browser…" );
					break;
				case "slow_down":
					pollIntervalSeconds += 5;
					onStatus?.Invoke( "Waiting for authorization in browser…" );
					break;
				case "expired_token":
					throw new InvalidOperationException( "The device code has expired. Please try again." );
				case "access_denied":
					throw new InvalidOperationException( "Authorization was denied." );
				default:
					throw new InvalidOperationException( string.IsNullOrEmpty( poll.ErrorDescription )
						? $"Authorization failed: {poll.Error}"
						: poll.ErrorDescription );
			}
		}

		throw new OperationCanceledException( "Device flow polling was cancelled." );
	}


	private static string ExtractError( string json, string httpStatus )
	{
		try
		{
			var doc = JsonDocument.Parse( json );
			if ( doc.RootElement.TryGetProperty( "error_description", out var d ) && !string.IsNullOrEmpty( d.GetString() ) )
			{
				return d.GetString()!;
			}

			if ( doc.RootElement.TryGetProperty( "error", out var err ) && !string.IsNullOrEmpty( err.GetString() ) )
			{
				return err.GetString()!;
			}

			if ( doc.RootElement.TryGetProperty( "message", out var m ) && !string.IsNullOrEmpty( m.GetString() ) )
			{
				return m.GetString()!;
			}
		}
		catch { /* fall through */ }
		return httpStatus is "404" or "NotFound"
			? "GitHub returned 404. Verify your Client ID and that 'Device Flow' is enabled."
			: $"GitHub returned HTTP {httpStatus}.";
	}
}
