using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using CopilotUsage.Models;

namespace CopilotUsage.ViewModels;

internal sealed class UsageViewModel : INotifyPropertyChanged
{
	private int m_UsedRequests;
	private int m_LimitRequests = 300;
	private DateTimeOffset? m_ResetAt;
	private DateTime m_LastUpdated;
	private string? m_ErrorMessage;
	private bool m_IsLoading;
	private bool m_IsDemoMode;
	private DateTime? m_OverrideNow;


	/// <summary>
	/// When set, all month-progress properties use this date instead of <see cref="DateTime.Now"/>.
	/// Used by demo mode to simulate different points in the month.
	/// </summary>
	public DateTime? OverrideNow
	{
		get => m_OverrideNow;
		set
		{
			m_OverrideNow = value;
			NotifyMonthProperties();
		}
	}

	private DateTime Now => m_OverrideNow ?? DateTime.Now;


	public int UsedRequests
	{
		get => m_UsedRequests;
		set
		{
			m_UsedRequests = value;
			OnPropertyChanged();
			OnPropertyChanged( nameof( UsagePercent ) );
			OnPropertyChanged( nameof( UsageSummary ) );
			OnPropertyChanged( nameof( UsageColorZone ) );
			OnPropertyChanged( nameof( HasData ) );
		}
	}

	public int LimitRequests
	{
		get => m_LimitRequests;
		set
		{
			m_LimitRequests = value;
			OnPropertyChanged();
			OnPropertyChanged( nameof( UsagePercent ) );
			OnPropertyChanged( nameof( UsageSummary ) );
			OnPropertyChanged( nameof( UsageColorZone ) );
		}
	}

	public double UsagePercent =>
		m_LimitRequests > 0 ? Math.Min( 100.0, m_UsedRequests * 100.0 / m_LimitRequests ) : 0;

	/// <summary>Returns "Green", "Yellow", or "Red" for XAML DataTrigger colour selection.</summary>
	public string UsageColorZone => UsagePercent switch
	{
		< 60 => "Green",
		< 80 => "Yellow",
		_ => "Red",
	};

	public bool IsUnlimited => m_LimitRequests == 0;

	public string UsageSummary =>
		IsUnlimited
			? "Unlimited"
			: $"{m_UsedRequests} / {m_LimitRequests} ({UsagePercent:0}%)";

	// Month progress — instance properties so OnPropertyChanged works correctly
	public double MonthProgressPercent
	{
		get
		{
			var now = Now;
			int days = DateTime.DaysInMonth( now.Year, now.Month );
			return Math.Min( 100.0, ( now.Day - 1 + now.TimeOfDay.TotalDays ) / days * 100.0 );
		}
	}

	public string MonthProgressSummary
	{
		get
		{
			var now = Now;
			int days = DateTime.DaysInMonth( now.Year, now.Month );
			return $"{now.Day} / {days} days ({MonthProgressPercent:0}%)";
		}
	}

	/// <summary>Label for the month bar, always in English, no extra spaces around brackets.</summary>
	public string MonthProgressLabel
	{
		get
		{
			var month = Now.ToString( "MMMM", CultureInfo.InvariantCulture );
			return $"Month Progress ({month})";
		}
	}

	public DateTimeOffset? ResetAt
	{
		get => m_ResetAt;
		set
		{
			m_ResetAt = value;
			OnPropertyChanged();
			OnPropertyChanged( nameof( ResetAtSummary ) );
		}
	}

	public string ResetAtSummary
	{
		get
		{
			if ( !m_ResetAt.HasValue )
			{
				return string.Empty;
			}

			return "Resets " + m_ResetAt.Value.ToLocalTime().ToString( "MMMM d", CultureInfo.InvariantCulture );
		}
	}

	public DateTime LastUpdated
	{
		get => m_LastUpdated;
		set
		{
			m_LastUpdated = value;
			OnPropertyChanged();
			OnPropertyChanged( nameof( LastUpdatedSummary ) );
		}
	}

	public string LastUpdatedSummary =>
		m_LastUpdated == default
			? string.Empty
			: "Last updated " + m_LastUpdated.ToString( "HH:mm:ss", CultureInfo.InvariantCulture );

	public string? ErrorMessage
	{
		get => m_ErrorMessage;
		set
		{
			m_ErrorMessage = value;
			OnPropertyChanged();
			OnPropertyChanged( nameof( HasError ) );
			OnPropertyChanged( nameof( HasData ) );
		}
	}

	public bool HasError => !string.IsNullOrEmpty( m_ErrorMessage );

	public bool HasData => !HasError && m_LastUpdated != default;

	public bool IsLoading
	{
		get => m_IsLoading;
		set { m_IsLoading = value; OnPropertyChanged(); }
	}

	public bool IsDemoMode
	{
		get => m_IsDemoMode;
		set { m_IsDemoMode = value; OnPropertyChanged(); }
	}


	public void UpdateFromData( CopilotUsageData data )
	{
		if ( data.IsUnavailable )
		{
			ErrorMessage = "Premium request quota data is not available for your Copilot subscription type.";
			LastUpdated = DateTime.Now;
			NotifyMonthProperties();
			return;
		}

		if ( data.Unlimited )
		{
			LimitRequests = 0;
			UsedRequests = 0;
		}
		else
		{
			LimitRequests = data.Limit > 0 ? data.Limit : 300;
			UsedRequests = data.Used;
		}

		ResetAt = data.ResetAt;
		LastUpdated = DateTime.Now;
		ErrorMessage = null;
		NotifyMonthProperties();
	}

	private void NotifyMonthProperties()
	{
		OnPropertyChanged( nameof( MonthProgressPercent ) );
		OnPropertyChanged( nameof( MonthProgressSummary ) );
		OnPropertyChanged( nameof( MonthProgressLabel ) );
	}


	public event PropertyChangedEventHandler? PropertyChanged;

	private void OnPropertyChanged( [CallerMemberName] string? name = null )
		=> PropertyChanged?.Invoke( this, new PropertyChangedEventArgs( name ) );
}
