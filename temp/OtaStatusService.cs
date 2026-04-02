using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using Trimble.RTN.Framework.API.Logging.V2;
using Trimble.RTN.Service.DeviceRegistration.Client;
using Trimble.RTN.Service.Minerva.Common.Tools;
using Trimble.RTN.Service.Minerva.Common.UserAgencyClient;
using Trimble.RTN.Service.OAuthProvider.Clients;
using Trimble.RTN.Service.Subscriptions.Api.DTOs.ThirdParty;
using Trimble.RTN.Service.Subscriptions.Client.DTOs;
using Trimble.RTN.Service.UserAgency.Abstractions;
using Trimble.RTN.Service.UserAgency.Authentication;

namespace Trimble.RTN.Service.Subscriptions.Api.Services;

/// <summary>
/// Implementation of the OTA status service that communicates with the RTX BSM API.
/// </summary>
internal sealed class OtaStatusService(
	IRtxBsmClient rtxBsmClient,
	ITapStoreMinervaClient tapStoreMinervaClient,
	IDeviceRegistrationClient deviceRegistrationClient,
	IUserAgencyClient userAgencyClient,
	IJwtTokenExtractor jwtTokenExtractor,
	ITokenClient tokenClient,
	IStructuredTrimbleLogger<OtaStatusService> logger ) : IOtaStatusService
{
	/// <summary>
	/// The channel to ignore when processing subscription requests (IP delivery, not satellite).
	/// </summary>
	private const string IgnoredChannel = "RTXIP";

	/// <summary>
	/// Represents the vendor identifier string used for resending subscriptions by Minerva (Trimble Positioning Hub) in RTX BSM.
	/// </summary>
	private const string MinervaVendorUUID = "TPHUB_APP";

	/// <summary>
	/// Contains the list of allowed VendorUUIDs for processing subscription requests from RTX BSM.
	/// Only subscription requests with a VendorUUID in this list will be considered when fetching statuses and histories, all others will be filtered out.
	/// </summary>
	private static readonly string[] AllowedVendorUUIDs = [
		"BRDC_APP",			// ID used by the TAP Store
		MinervaVendorUUID,	// ID used by Minerva (Trimble Positioning Hub) for resending subscriptions
	];

	/// <summary>
	/// Maximum time difference between statuses (in hours) before returning Unknown.
	/// If statuses for a serial number differ and the time range exceeds this threshold,
	/// the merged status will be set to Unknown.
	/// </summary>
	private const int MaxStatusTimeDifferenceHours = 5;

	/// <summary>
	/// Maximum number of concurrent API calls to fetch latest status data.
	/// Limits parallelism to avoid overwhelming the RTX BSM API.
	/// </summary>
	private const int MaxConcurrentApiCalls = 10;

	private readonly IRtxBsmClient m_RtxBsmClient = rtxBsmClient ?? throw new ArgumentNullException( nameof( rtxBsmClient ) );
	private readonly ITapStoreMinervaClient m_TapStoreMinervaClient = tapStoreMinervaClient ?? throw new ArgumentNullException( nameof( tapStoreMinervaClient ) );
	private readonly IDeviceRegistrationClient m_DeviceRegistrationClient = deviceRegistrationClient ?? throw new ArgumentNullException( nameof( deviceRegistrationClient ) );
	private readonly IUserAgencyClient m_UserAgencyClient = userAgencyClient ?? throw new ArgumentNullException( nameof( userAgencyClient ) );
	private readonly IJwtTokenExtractor m_JwtTokenExtractor = jwtTokenExtractor ?? throw new ArgumentNullException( nameof( jwtTokenExtractor ) );
	private readonly ITokenClient m_TokenClient = tokenClient ?? throw new ArgumentNullException( nameof( tokenClient ) );
	private readonly IStructuredTrimbleLogger<OtaStatusService> m_Logger = logger ?? throw new ArgumentNullException( nameof( logger ) );


	/// <summary>
	/// Internal record to hold status and time for merging purposes.
	/// </summary>
	private sealed record StatusEntry( OtaActivationStatus Status, DateTime TimeOfStatus );


	/// <inheritdoc />
	public async IAsyncEnumerable<OtaLatestStatusDto> GetLatestStatusBySerialsAsync(
		IEnumerable<string> serialNumbers,
		[EnumeratorCancellation] CancellationToken cancellationToken = default )
	{
		ArgumentNullException.ThrowIfNull( serialNumbers );

		var serialList = serialNumbers.ToList();
		if ( serialList.Count == 0 )
		{
			throw new ArgumentException( "At least one serial number must be provided.", nameof( serialNumbers ) );
		}

		// Validate all serial numbers and throw an exception if any is invalid.
		foreach ( var serialNumber in serialList )
		{
			SubscriptionValidator.ValidateSerialNumber( serialNumber );
		}

		// Step 1: Get subscription requests from RTX BSM.
		var subscriptionRequests = await m_RtxBsmClient.GetSubscriptionRequestsAsync( serialList, cancellationToken ).ConfigureAwait( false );

		if ( subscriptionRequests.Count == 0 )
		{
			yield break;
		}

		// Step 2:
		// - Filter out:
		//   * killSignal entries, as we don't want customers to see these
		//   * RTXIP channel, as this doesn't go out to customers
		//   * VendorUUIDs which customers should not see, like for internal testing ("TERRASAT_UI") or anti-piracy entries ("TERRASAT_CONSOLE")
		// - Then group by serial number.
		var groupedSubscriptionRequests = subscriptionRequests
			.Where( r => !r.KillSignal )
			.Where( r => !r.BroadcastChannels.Contains( IgnoredChannel, StringComparer.OrdinalIgnoreCase ) )
			.Where( r => AllowedVendorUUIDs.Contains( r.VendorUUID, StringComparer.OrdinalIgnoreCase ) )
			.GroupBy( r => r.FullReceiverSerialNumber );

		// Step 3: For each serial number, fetch all statuses, merge, and yield immediately.
		foreach ( var requestsOfOneSerial in groupedSubscriptionRequests )
		{
			var serialNumber = requestsOfOneSerial.Key;

			// Get latest status for each vendor reference (parallelized with concurrency limit).
			var statusesBag = new ConcurrentBag<StatusEntry>();
			await Parallel.ForEachAsync(
				requestsOfOneSerial,
				new ParallelOptions
				{
					MaxDegreeOfParallelism = MaxConcurrentApiCalls,
					CancellationToken = cancellationToken
				},
				async ( request, ct ) =>
				{
					var latestStatus = await m_RtxBsmClient.GetLatestStatusAsync( request.VendorReference, ct ).ConfigureAwait( false );

					if ( latestStatus == null )
					{
						// Empty data returned - this is unexpected, log and mark as Error.
						m_Logger.LogError( $"No status data returned for vendorReference '{request.VendorReference}' (serial: {serialNumber}). Marking as Error." );
						statusesBag.Add( new StatusEntry( OtaActivationStatus.Error, DateTime.UtcNow ) );
						return;
					}

					var status = MapIntToStatus( latestStatus.Status );
					var timeOfStatus = DateTime.SpecifyKind( latestStatus.TimeOfStatus, DateTimeKind.Utc );
					statusesBag.Add( new StatusEntry( status, timeOfStatus ) );
				}
			).ConfigureAwait( false );

			// Merge and yield the result for this serial number immediately. 
			var merged = MergeStatuses( serialNumber, statusesBag.ToList() );
			yield return new OtaLatestStatusDto
			{
				SerialNumber = serialNumber,
				Status = merged.Status,
				TimeOfStatusUtc = merged.TimeOfStatus
			};
		}
	}


	/// <inheritdoc />
	public async Task<IEnumerable<OtaStatusHistoryEventDto>?> GetHistoryBySerialsAsync( IEnumerable<string> serialNumbers, CancellationToken cancellationToken = default )
	{
		ArgumentNullException.ThrowIfNull( serialNumbers );

		var serialList = serialNumbers.ToList();
		if ( serialList.Count == 0 )
		{
			throw new ArgumentException( "At least one serial number must be provided.", nameof( serialNumbers ) );
		}

		// Validate all serial numbers and throw an exception if any is invalid.
		foreach ( var serialNumber in serialList )
		{
			SubscriptionValidator.ValidateSerialNumber( serialNumber );
		}

		// Step 1: Get subscription requests for all serial numbers from the RTX BSM API.
		var subscriptionRequests = await m_RtxBsmClient.GetSubscriptionRequestsAsync( serialList, cancellationToken ).ConfigureAwait( false );

		if ( subscriptionRequests.Count == 0 )
		{
			// Serial number not found in RTX BSM - return null (404).
			return null;
		}

		// Step 2: Filter out:
		//   * killSignal entries, as we don't want customers to see these
		//   * RTXIP channel, as this doesn't go out to customers
		//   * VendorUUIDs which customers should not see, like for internal testing ("TERRASAT_UI") or anti-piracy entries ("TERRASAT_CONSOLE")
		var relevantRequests = subscriptionRequests
			.Where( r => !r.KillSignal )
			.Where( r => !r.BroadcastChannels.Contains( IgnoredChannel, StringComparer.OrdinalIgnoreCase ) )
			.Where( r => AllowedVendorUUIDs.Contains( r.VendorUUID, StringComparer.OrdinalIgnoreCase ) )
			.ToList();

		if ( relevantRequests.Count == 0 )
		{
			// No relevant subscription requests found - return empty list.
			return [];
		}

		// Step 3: Get device names for all serial numbers from DeviceRegistration service.
		var relevantSerials = relevantRequests.Select( r => r.FullReceiverSerialNumber ).Distinct().ToList();
		var deviceNameMap = await GetDeviceNamesAsync( relevantSerials, cancellationToken ).ConfigureAwait( false );

		// Step 4: For each subscription request, get the status history in parallel.
		var historyEventsBag = new ConcurrentBag<OtaStatusHistoryEventDto>();

		await Parallel.ForEachAsync(
			relevantRequests,
			new ParallelOptions
			{
				MaxDegreeOfParallelism = MaxConcurrentApiCalls,
				CancellationToken = cancellationToken
			},
			async ( request, ct ) =>
			{
				var serialNumber = request.FullReceiverSerialNumber;
				var deviceName = deviceNameMap.GetValueOrDefault( serialNumber );
				var serviceTypeName = ServiceTypeMapper.GetDisplayName( request.ServiceType );
				var startDateUtc = DateTime.SpecifyKind( request.StartDate, DateTimeKind.Utc );
				var endDateUtc = DateTime.SpecifyKind( request.ExpiryDate, DateTimeKind.Utc );

				// Get status history for this vendor reference.
				var statusHistory = await m_RtxBsmClient.GetStatusHistoryAsync( request.VendorReference, ct ).ConfigureAwait( false );

				foreach ( var statusEntry in statusHistory )
				{
					// Skip deleted entries.
					if ( statusEntry.Deleted )
					{
						continue;
					}

					var status = MapIntToStatus( statusEntry.Status );
					var timeOfStatusUtc = DateTime.SpecifyKind( statusEntry.CreatedAt, DateTimeKind.Utc );

					var historyEvent = new OtaStatusHistoryEventDto
					{
						SerialNumber = serialNumber,
						DeviceName = deviceName,
						Status = status,
						TimeOfStatusUtc = timeOfStatusUtc,
						DataChannel = statusEntry.DataChannel,
						ServiceType = serviceTypeName,
						StartDateUtc = startDateUtc,
						EndDateUtc = endDateUtc
					};
					historyEvent.Id = GenerateStatusHistoryEventId( historyEvent );

					historyEventsBag.Add( historyEvent );
				}
			}
		).ConfigureAwait( false );

		// Step 5: Sort by time of status ascending and return.
		return historyEventsBag.OrderBy( e => e.TimeOfStatusUtc );
	}


	/// <summary>
	/// Generates a unique identifier for an OTA status history event based on key fields.
	/// Uses SHA256 hash to create a deterministic, collision-resistant ID.
	/// </summary>
	private static string GenerateStatusHistoryEventId( OtaStatusHistoryEventDto historyEvent )
	{
		// Create a composite key from identifying fields.
		var compositeKey = $"{historyEvent.SerialNumber}|{historyEvent.Status}|{historyEvent.TimeOfStatusUtc:O}|{historyEvent.DataChannel}|{historyEvent.ServiceType}|{historyEvent.StartDateUtc:O}|{historyEvent.EndDateUtc:O}";

		// Generate SHA256 hash.
		var hashBytes = SHA256.HashData( Encoding.UTF8.GetBytes( compositeKey ) );

		// Convert to hexadecimal string.
		return Convert.ToHexString( hashBytes );
	}


	/// <summary>
	/// Gets device names from the DeviceRegistration service for the given serial numbers.
	/// Uses an application token to access the internal device names endpoint.
	/// Returns a dictionary mapping serial numbers to device names (or null if no device found for that serial).
	/// </summary>
	private async Task<Dictionary<string, string?>> GetDeviceNamesAsync( IEnumerable<string> serialNumbers, CancellationToken cancellationToken )
	{
		var serialList = serialNumbers.ToList();
		var serialToDeviceNameMap = new Dictionary<string, string?>( StringComparer.Ordinal );

		if ( serialList.Count == 0 )
		{
			return serialToDeviceNameMap;
		}

		try
		{
			// Get JWT token from Trimble Identity (needed to get an application token).
			var tidTokenResponse = await m_TokenClient.GetTidTokenAsync( cancellationToken: cancellationToken ).ConfigureAwait( false );

			// Get an application token to call the DeviceRegistration service.
			var applicationTokenResponse = await m_TokenClient.GetApplicationTokenAsync( tidTokenResponse.AccessToken, cancellationToken ).ConfigureAwait( false );

			var deviceNames = await m_DeviceRegistrationClient.GetDeviceNamesBySerialNumbersAsync( serialList, applicationTokenResponse.AccessToken, cancellationToken ).ConfigureAwait( false );

			// Build the dictionary from the response.
			foreach ( var deviceName in deviceNames )
			{
				serialToDeviceNameMap[deviceName.SerialNumber] = deviceName.DeviceName;
			}

			// Log warnings for serial numbers not found.
			foreach ( var serial in serialList.Where( s => !serialToDeviceNameMap.ContainsKey( s ) ) )
			{
				m_Logger.LogWarn( $"Device with serial number '{serial}' not found in DeviceRegistration service." );
			}

			return serialToDeviceNameMap;
		}
		catch ( Exception ex ) when ( ex is not OperationCanceledException )
		{
			m_Logger.LogError( ex, "Failed to retrieve device names from DeviceRegistration service." );

			// Return empty map on failure - .GetValueOrDefault() will return null for missing keys.
			return serialToDeviceNameMap;
		}
	}


	/// <inheritdoc />
	public async Task<IEnumerable<OtaStatusHistoryEventDto>> GetHistoryByUserRoleAsync( TrimbleUser user, CancellationToken cancellationToken = default )
	{
		ArgumentNullException.ThrowIfNull( user );

		// Extract JWT token from the current HTTP context.
		var jwtToken = m_JwtTokenExtractor.ExtractJwtToken();
		if ( string.IsNullOrEmpty( jwtToken ) )
		{
			throw new SecurityTokenValidationException( $"No JWT token available for Device Registration API call. Cannot query device registrations for account '{user.AccountId}' (user '{user.Tid}')." );
		}

		var isDealer = user.HasRole( Role.Dealer );
		var isAccountAdmin = user.HasRole( Role.AccountAdmin );

		if ( isDealer )
		{
			return await GetHistoryForDealerAsync( user, jwtToken, cancellationToken ).ConfigureAwait( false );
		}
		else if ( isAccountAdmin )
		{
			return await GetHistoryForAccountAdminAsync( user, jwtToken, cancellationToken ).ConfigureAwait( false );
		}
		else
		{
			throw new UnauthorizedAccessException( $"User '{user.Tid}' does not have a role that allows access to OTA status history. Required role: Dealer or Account Admin." );
		}
	}


	/// <summary>
	/// Retrieves OTA history for a dealer by fetching all devices across all customer accounts.
	/// Only includes history for subscriptions where the ResellerId matches the dealer's CustAccountId.
	/// </summary>
	private async Task<IEnumerable<OtaStatusHistoryEventDto>> GetHistoryForDealerAsync( TrimbleUser user, string jwtToken, CancellationToken cancellationToken )
	{
		m_Logger.LogDebug( $"User '{user.Tid}' is a Dealer. Retrieving OTA history for all customer accounts." );

		// Get the dealer's CustAccountId from UserAgency (needed to filter subscriptions by ResellerId).
		var dealerId = await m_UserAgencyClient.GetCustAccountIdAsync( user.Tid, jwtToken, cancellationToken ).ConfigureAwait( false );

		// Get all customer accounts with users for the dealer.
		var customerAccounts = await m_UserAgencyClient.GetDealerCustomerAccountsWithUsersAsync( jwtToken, cancellationToken ).ConfigureAwait( false );
		m_Logger.LogDebug( $"Found {customerAccounts.Count()} customer accounts for dealer '{user.Tid}'." );

		var allSerialNumbers = new List<string>();

		// For each onboarded customer account, get device registrations.
		foreach ( var customerAccount in customerAccounts )
		{
			if ( customerAccount.Id is null || customerAccount.Users?.Any() == false )
			{
				m_Logger.LogInfo( $"Skipping customer '{customerAccount.CustAccountId}'. Customer is not onboarded yet in Minerva." );
				continue;
			}

			var accountId = customerAccount.Id.Value;
			var deviceRegistrations = await m_DeviceRegistrationClient.GetDeviceRegistrationsByAccountAsync( accountId, jwtToken, cancellationToken ).ConfigureAwait( false );
			var serialNumbers = deviceRegistrations.Select( dr => dr.SerialNumber ).ToList();

			m_Logger.LogDebug( $"Found {serialNumbers.Count} device(s) in customer account '{customerAccount.CustAccountId}' ({customerAccount.AccountName})." );
			allSerialNumbers.AddRange( serialNumbers );
		}

		var distinctSerialNumbers = allSerialNumbers.Distinct().ToList();

		if ( distinctSerialNumbers.Count == 0 )
		{
			m_Logger.LogDebug( $"No device registrations found across all customer accounts for dealer '{user.Tid}'." );
			return [];
		}

		// Filter serial numbers to only those with subscriptions sold by this dealer (ResellerId match).
		var filteredSerialNumbers = await FilterSerialsByDealerAsync( distinctSerialNumbers, dealerId, cancellationToken ).ConfigureAwait( false );

		if ( filteredSerialNumbers.Count == 0 )
		{
			m_Logger.LogDebug( $"No subscriptions with ResellerId '{dealerId}' found for dealer '{user.Tid}' after filtering {distinctSerialNumbers.Count} device(s)." );
			return [];
		}

		m_Logger.LogDebug( $"Filtered {distinctSerialNumbers.Count} device(s) to {filteredSerialNumbers.Count} with ResellerId '{dealerId}' for dealer '{user.Tid}'. Retrieving OTA history." );

		// Get OTA history for all serial numbers. Convert null to empty list (null means no subscription requests found).
		var historyEvents = await GetHistoryBySerialsAsync( filteredSerialNumbers, cancellationToken ).ConfigureAwait( false );
		return historyEvents ?? [];
	}



	/// <summary>
	/// Filters serial numbers to only those that have TAP Store subscriptions where the ResellerId matches the dealer's CustAccountId.
	/// This ensures a dealer only sees OTA history for subscriptions they sold.
	/// </summary>
	private async Task<List<string>> FilterSerialsByDealerAsync( List<string> serialNumbers, string dealerId, CancellationToken cancellationToken )
	{
		// Query TAP Store for RTX subscriptions in chunks (max 10 per request) and filter in parallel.
		var filteredSerialNumbersBag = new ConcurrentBag<string>();

		// Create chunks of serial numbers for parallel processing.
		var serialNumberChunks = serialNumbers.Chunk( TapStoreMinervaClient.MaxSerialNumbersPerRequest );

		await Parallel.ForEachAsync(
			serialNumberChunks,
			new ParallelOptions
			{
				MaxDegreeOfParallelism = MaxConcurrentApiCalls,
				CancellationToken = cancellationToken
			},
			async ( serialNumberChunk, ct ) =>
			{
				var subscriptions = await m_TapStoreMinervaClient.GetRtxSubscriptionsBySerialsAsync( serialNumberChunk, ct ).ConfigureAwait( false );

				// Filter and add only serial numbers with matching ResellerId.
				foreach ( var subscription in subscriptions.Where( s => s.ResellerId == dealerId ) )
				{
					filteredSerialNumbersBag.Add( subscription.SerialNumber );
				}
			}
		).ConfigureAwait( false );

		// Return distinct serial numbers.
		return filteredSerialNumbersBag.Distinct().ToList();
	}


	/// <summary>
	/// Retrieves OTA history for an account admin by fetching all devices in their account.
	/// </summary>
	private async Task<IEnumerable<OtaStatusHistoryEventDto>> GetHistoryForAccountAdminAsync( TrimbleUser user, string jwtToken, CancellationToken cancellationToken )
	{
		m_Logger.LogDebug( $"User '{user.Tid}' is an Account Admin. Retrieving OTA history for account '{user.AccountId}'." );

		// Get all device registrations for the account.
		var deviceRegistrations = await m_DeviceRegistrationClient.GetDeviceRegistrationsByAccountAsync( user.AccountId, jwtToken, cancellationToken ).ConfigureAwait( false );
		var serialNumbers = deviceRegistrations.Select( dr => dr.SerialNumber ).Distinct().ToList();

		if ( serialNumbers.Count == 0 )
		{
			m_Logger.LogDebug( $"No device registrations found for account '{user.AccountId}'." );
			return [];
		}

		m_Logger.LogDebug( $"Found {serialNumbers.Count} device(s) in account '{user.AccountId}'. Retrieving OTA history." );

		// Get OTA history for all serial numbers. Convert null to empty list (null means no subscription requests found).
		var historyEvents = await GetHistoryBySerialsAsync( serialNumbers, cancellationToken ).ConfigureAwait( false );
		return historyEvents ?? [];
	}


	/// <inheritdoc />
	public async Task<bool> RequestActivationResendAsync( string serialNumber, CancellationToken cancellationToken = default )
	{
		SubscriptionValidator.ValidateSerialNumber( serialNumber );

		// Step 1: Get existing subscription requests from RTX BSM.
		var subscriptionRequests = await m_RtxBsmClient.GetSubscriptionRequestsAsync( [serialNumber], cancellationToken ).ConfigureAwait( false );

		if ( subscriptionRequests.Count == 0 )
		{
			// Serial number not found in RTX BSM.
			return false;
		}

		// Filter out kill signals and unknown VendorUUIDs - we never want to resend these.
		// Also filter out already resent subscriptions (those with the Minerva VendorUUID).
		var validRequests = subscriptionRequests
			.Where( r => !r.KillSignal )
			.Where( r => AllowedVendorUUIDs.Contains( r.VendorUUID, StringComparer.OrdinalIgnoreCase ) )
			.Where( r => r.VendorUUID != MinervaVendorUUID )
			.ToList();

		if ( validRequests.Count == 0 )
		{
			m_Logger.LogWarn( $"No valid subscription requests found for serial number '{serialNumber}' (all are kill signals or have unknown VendorUUIDs). Cannot resend." );
			return false;
		}

		// Step 2: Query TAP Store for the currently active/future subscriptions to filter out expired ones from RTX BSM.
		IEnumerable<TapStoreRtxSubscriptionItemDto> tapStoreSubscriptions;
		try
		{
			tapStoreSubscriptions = await m_TapStoreMinervaClient.GetRtxSubscriptionsBySerialsAsync( [serialNumber], cancellationToken ).ConfigureAwait( false );
		}
		catch ( Exception ex ) when ( ex is not OperationCanceledException )
		{
			m_Logger.LogError( ex, $"Failed to retrieve TAP Store subscriptions for serial number '{serialNumber}'. Cannot resend." );
			throw;
		}

		// Parse dates and keep only actively running subscriptions (started but not yet expired).
		var today = DateTime.UtcNow.Date;
		var activeDateRanges = new List<(DateTime Start, DateTime End)>();
		foreach ( var tapSub in tapStoreSubscriptions )
		{
			if ( SubscriptionDataMapper.TryParseDate( tapSub.StartDate, out var start ) &&
				 SubscriptionDataMapper.TryParseDate( tapSub.EndDate, out var end ) &&
				 start.Date <= today && end.Date >= today )
			{
				activeDateRanges.Add( (start.Date, end.Date) );
			}
		}

		if ( activeDateRanges.Count == 0 )
		{
			m_Logger.LogWarn( $"No active subscriptions found in TAP Store for serial number '{serialNumber}'. Nothing to resend." );
			return false;
		}

		// Step 3: Match RTX BSM requests against active TAP Store subscriptions by date range.
		var requestsToResend = validRequests
			.Where( r => activeDateRanges.Any( range =>
				r.StartDate.Date == range.Start &&
				r.ExpiryDate.Date == range.End ) )
			.ToList();

		if ( requestsToResend.Count == 0 )
		{
			m_Logger.LogWarn( $"No RTX BSM subscription requests match the active TAP Store subscriptions for serial number '{serialNumber}'. Nothing to resend." );
			return false;
		}

		// Step 4: Build new subscription requests with overridden vendorUUID and a fresh vendorReference.
		var timestamp = DateTime.UtcNow.ToString( "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture );
		var resendRequests = requestsToResend
			.Select( r => r with
			{
				VendorUUID = MinervaVendorUUID,
				VendorReference = $"{r.VendorReference}_{timestamp}",
				// Set HoursOfService to 1 if it wasn't set before, otherwise the RTX BSM API will reject it.
				HoursOfService = r.HoursOfService == 0 ? 1 : r.HoursOfService,
			} )
			.ToList();

		// Step 5: POST each resend request individually to RTX BSM (the API accepts one request at a time).
		foreach ( var resendRequest in resendRequests )
		{
			await m_RtxBsmClient.ResendSubscriptionRequestAsync( resendRequest, cancellationToken ).ConfigureAwait( false );
		}

		var channels = string.Join( ", ", resendRequests.SelectMany( r => r.BroadcastChannels ) );
		m_Logger.LogInfo( $"Successfully submitted {resendRequests.Count} subscription resend request(s) for serial number '{serialNumber}'. Channels: [{channels}]." );
		return true;
	}


	/// <summary>
	/// Merges multiple statuses for a single serial number into one.
	/// </summary>
	/// <remarks>
	/// Merging logic:
	/// <list type="bullet">
	///   <item>If all statuses match, return that status with the latest timeOfStatus.</item>
	///   <item>If statuses differ and the time range exceeds 5 hours, return Unknown.</item>
	///   <item>If statuses differ within 5 hours, return the lowest status value (e.g., Pending beats Delivered).</item>
	/// </list>
	/// </remarks>
	private StatusEntry MergeStatuses( string serialNumber, List<StatusEntry> statuses )
	{
		if ( statuses.Count == 0 )
		{
			return new StatusEntry( OtaActivationStatus.Unknown, DateTime.UtcNow );
		}

		if ( statuses.Count == 1 )
		{
			return statuses[0];
		}

		// Get the latest time (for the result).
		var latestTime = statuses.Max( s => s.TimeOfStatus );

		// Check if all statuses are the same.
		var distinctStatuses = statuses.Select( s => s.Status ).Distinct().ToList();

		if ( distinctStatuses.Count == 1 )
		{
			// All statuses match - return that status with the latest time.
			return new StatusEntry( distinctStatuses[0], latestTime );
		}

		// Statuses differ - check the time range.
		var earliestTime = statuses.Min( s => s.TimeOfStatus );
		var timeRange = latestTime - earliestTime;

		var statusDetails = string.Join( ", ", statuses.Select( s => $"{s.Status} @ {s.TimeOfStatus:HH:mm:ss.fff}" ) );

		if ( timeRange.TotalHours > MaxStatusTimeDifferenceHours )
		{
			// Time difference exceeds threshold - return Unknown.
			m_Logger.LogInfo( $"Merging statuses for '{serialNumber}': [{statusDetails}]. Time range {timeRange.TotalHours:F2}h exceeds threshold. Returning Unknown." );
			return new StatusEntry( OtaActivationStatus.Unknown, latestTime );
		}

		// Within time threshold - return the lowest status value (most conservative).
		var lowestStatus = distinctStatuses.Min();
		m_Logger.LogInfo( $"Merging statuses for '{serialNumber}': [{statusDetails}]. Time range {timeRange.TotalHours:F2}h. Returning '{lowestStatus}'." );
		return new StatusEntry( lowestStatus, latestTime );
	}


	/// <summary>
	/// Maps an integer status value from RTX BSM to the OtaActivationStatus enum.
	/// </summary>
	private OtaActivationStatus MapIntToStatus( int statusValue )
	{
		if ( Enum.IsDefined( typeof( OtaActivationStatus ), statusValue ) )
		{
			return (OtaActivationStatus) statusValue;
		}

		m_Logger.LogError( $"Unknown OTA activation status value '{statusValue}' received from RTX BSM API! This should not happen. Continuing now by returning 'Unknown' status." );
		return OtaActivationStatus.Unknown;
	}

}
