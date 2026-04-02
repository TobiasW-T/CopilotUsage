using ThreadingTimer = System.Threading.Timer;

using CopilotUsage.Helpers;
using CopilotUsage.Services;
using CopilotUsage.ViewModels;
using CopilotUsage.Views;

namespace CopilotUsage;

internal sealed class TrayApplicationContext : IDisposable
{
	private readonly GitHubCopilotService m_CopilotService;
	private readonly UsageViewModel m_ViewModel;
	private readonly bool m_IsDemoMode;

	private NotifyIcon? m_NotifyIcon;
	private ThreadingTimer? m_RefreshTimer;
	private UsagePopupWindow? m_PopupWindow;
	private bool m_IsRefreshing;

	// Demo mode state
	private int m_DemoUsageStep;
	private int m_DemoDateIndex;
	private int m_DemoTickCount;

	// Demo date snapshots: interesting points in the month calendar to cycle through.
	private static readonly DateTime[] DemoDates =
	[
		new DateTime( 2026, 1, 1, 0, 0, 0, DateTimeKind.Local ),   // Jan  1 — very start of month
		new DateTime( 2026, 2, 14, 12, 0, 0, DateTimeKind.Local ),  // Feb 14 — mid-month
		new DateTime( 2026, 2, 28, 23, 30, 0, DateTimeKind.Local ), // Feb 28 — last day of short month
		new DateTime( 2026, 3, 1, 0, 30, 0, DateTimeKind.Local ),   // Mar  1 — very start of month
		new DateTime( 2026, 3, 15, 12, 0, 0, DateTimeKind.Local ),  // Mar 15 — mid-month
		new DateTime( 2026, 6, 30, 23, 55, 0, DateTimeKind.Local ), // Jun 30 — last day of 30-day month
		new DateTime( 2026, 12, 31, 23, 59, 0, DateTimeKind.Local ),// Dec 31 — last day of year
	];

	// One demo tick every 2 s; advance the date every 5 ticks (= every 10 s)
	private const int DemoDateTickInterval = 5;


	public TrayApplicationContext( GitHubCopilotService copilotService, bool isDemoMode = false )
	{
		m_CopilotService = copilotService ?? throw new ArgumentNullException( nameof( copilotService ) );
		m_ViewModel = new UsageViewModel();
		m_IsDemoMode = isDemoMode;

		InitializeTrayIcon();

		if ( m_IsDemoMode )
		{
			StartDemoTimer();
		}
		else
		{
			RestartTimer();
			_ = RefreshAsync();
		}
	}


	private void InitializeTrayIcon()
	{
		var contextMenu = new ContextMenuStrip();
		contextMenu.Items.Add( "Refresh", null, async ( _, _ ) => await RefreshAsync().ConfigureAwait( false ) );
		contextMenu.Items.Add( "Settings…", null, ( _, _ ) => OpenSettings() );
		contextMenu.Items.Add( "About", null, ( _, _ ) =>
			System.Windows.Application.Current.Dispatcher.Invoke( () => new AboutWindow().ShowDialog() ) );
		contextMenu.Items.Add( new ToolStripSeparator() );
		contextMenu.Items.Add( "Exit", null, ( _, _ ) => System.Windows.Application.Current.Shutdown() );

		m_NotifyIcon = new NotifyIcon
		{
			Icon = TrayIconHelper.CreateUsageIcon( null ),
			Text = "Copilot Usage",
			Visible = true,
			ContextMenuStrip = contextMenu,
		};

		m_NotifyIcon.MouseClick += NotifyIcon_MouseClick;
	}


	private void NotifyIcon_MouseClick( object? sender, MouseEventArgs e )
	{
		if ( e.Button != MouseButtons.Left )
		{
			return;
		}

		System.Windows.Application.Current.Dispatcher.InvokeAsync( () =>
		{
			// Toggle: close if already open, open if closed
			if ( m_PopupWindow != null )
			{
				m_PopupWindow.Close();
				return;
			}

			ShowPopup();
			if ( !m_IsDemoMode )
			{
				_ = RefreshAsync();
			}
		} );
	}


	private void ShowPopup()
	{
		try
		{
			m_PopupWindow?.Close();
		}
		catch { /* already closing — ignore */ }

		m_PopupWindow = new UsagePopupWindow( m_ViewModel );
		m_PopupWindow.Closed += ( _, _ ) => m_PopupWindow = null;
		m_PopupWindow.Show();
		m_PopupWindow.Activate();
	}


	public async Task RefreshAsync()
	{
		if ( m_IsRefreshing )
		{
			return;
		}

		m_IsRefreshing = true;
		m_ViewModel.IsLoading = true;

		var settings = SettingsService.Load();

		try
		{
			var data = await m_CopilotService.GetUsageAsync( settings.AccessToken ).ConfigureAwait( true );

			m_ViewModel.UpdateFromData( data );

			if ( data.IsUnavailable )
			{
				UpdateTrayIconError();
			}
			else
			{
				UpdateTrayIcon();
			}
		}
		catch ( Exception ex )
		{
			m_ViewModel.ErrorMessage = ex.Message;
			UpdateTrayIconError();
		}
		finally
		{
			m_ViewModel.IsLoading = false;
			m_IsRefreshing = false;
		}
	}


	private void UpdateTrayIcon()
	{
		if ( m_NotifyIcon == null )
		{
			return;
		}

		var percent = m_ViewModel.UsagePercent;
		var oldIcon = m_NotifyIcon.Icon;
		m_NotifyIcon.Icon = TrayIconHelper.CreateUsageIcon( percent );

		var prefix = m_IsDemoMode ? "[DEMO] " : string.Empty;
		m_NotifyIcon.Text = $"{prefix}Copilot: {m_ViewModel.UsedRequests}/{m_ViewModel.LimitRequests} ({percent:0}%)";
		oldIcon?.Dispose();
	}


	private void UpdateTrayIconError()
	{
		if ( m_NotifyIcon == null )
		{
			return;
		}

		var oldIcon = m_NotifyIcon.Icon;
		m_NotifyIcon.Icon = TrayIconHelper.CreateUsageIcon( null );
		var prefix = m_IsDemoMode ? "[DEMO] " : string.Empty;
		m_NotifyIcon.Text = $"{prefix}Copilot Usage — error fetching data";
		oldIcon?.Dispose();
	}


	public void RestartTimer()
	{
		m_RefreshTimer?.Dispose();
		var settings = SettingsService.Load();
		var intervalMs = ( settings.RefreshIntervalMinutes > 0 ? settings.RefreshIntervalMinutes : 15 ) * 60 * 1000;
		m_RefreshTimer = new ThreadingTimer(
			async _ => await System.Windows.Application.Current.Dispatcher.InvokeAsync( RefreshAsync ),
			null, intervalMs, intervalMs );
	}


	/// <summary>
	/// Starts the demo-mode timer. Every 2 seconds the usage steps up by 5 % (wrapping at 100 → 0).
	/// Every 5 ticks (10 s) the displayed month date advances to the next preset snapshot.
	/// </summary>
	private void StartDemoTimer()
	{
		// Initialise with first demo values immediately
		ApplyDemoStep();

		m_RefreshTimer = new ThreadingTimer(
			_ => System.Windows.Application.Current.Dispatcher.Invoke( AdvanceDemoStep ),
			null, 2000, 2000 );
	}

	private void AdvanceDemoStep()
	{
		m_DemoTickCount++;

		// Advance date every DemoDateTickInterval ticks
		if ( m_DemoTickCount % DemoDateTickInterval == 0 )
		{
			m_DemoDateIndex = ( m_DemoDateIndex + 1 ) % DemoDates.Length;
		}

		// Advance usage by 5 % per tick, wrapping 100 → 0
		m_DemoUsageStep = ( m_DemoUsageStep + 1 ) % 21; // 0..20 → 0%..100%

		ApplyDemoStep();
	}

	private void ApplyDemoStep()
	{
		var usagePercent = m_DemoUsageStep * 5.0;  // 0, 5, 10, …, 100
		var limit = 300;
		var used = (int) Math.Round( usagePercent / 100.0 * limit );

		m_ViewModel.OverrideNow = DemoDates[m_DemoDateIndex];
		m_ViewModel.LimitRequests = limit;
		m_ViewModel.UsedRequests = used;
		m_ViewModel.ResetAt = new DateTimeOffset( DemoDates[m_DemoDateIndex] ).AddMonths( 1 ).ToUniversalTime();
		m_ViewModel.LastUpdated = DateTime.Now;
		m_ViewModel.ErrorMessage = null;
		m_ViewModel.IsDemoMode = true;

		UpdateTrayIcon();
	}


	private void OpenSettings()
	{
		System.Windows.Application.Current.Dispatcher.Invoke( () =>
		{
			var settingsWindow = new SettingsWindow();
			if ( settingsWindow.ShowDialog() == true )
			{
				RestartTimer();
				StartupHelper.ApplyStartupSetting( SettingsService.Load().StartWithWindows );
				_ = RefreshAsync();
			}
		} );
	}


	public void Dispose()
	{
		m_RefreshTimer?.Dispose();
		m_NotifyIcon?.Dispose();
	}
}
