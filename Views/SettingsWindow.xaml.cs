using System.Diagnostics;
using System.Windows;
using System.Windows.Controls;
using CopilotUsage.Services;
using WpfBrush = System.Windows.Media.Brush;
using WpfBrushes = System.Windows.Media.Brushes;

namespace CopilotUsage.Views;

public partial class SettingsWindow : Window
{
	private CancellationTokenSource? m_PollCts;
	private string? m_PendingAccessToken;


	internal SettingsWindow()
	{
		InitializeComponent();
		LoadSettings();
	}


	private void LoadSettings()
	{
		var settings = SettingsService.Load();

		ClientIdTextBox.Text = settings.GitHubClientId;
		StartupCheckBox.IsChecked = settings.StartWithWindows;

		foreach ( ComboBoxItem item in IntervalCombo.Items )
		{
			if ( item.Tag is string tag && int.TryParse( tag, out var val ) && val == settings.RefreshIntervalMinutes )
			{
				IntervalCombo.SelectedItem = item;
				break;
			}
		}

		if ( IntervalCombo.SelectedItem == null )
		{
			IntervalCombo.SelectedIndex = 2; // default: 15 min
		}

		if ( !string.IsNullOrWhiteSpace( settings.AccessToken ) )
		{
			SetAuthorizedState();
		}
		else if ( string.IsNullOrWhiteSpace( settings.GitHubClientId ) )
		{
			SetStatus( "Enter a Client ID above (click 'Register app ↗' to create one), then click Authorize.", WpfBrushes.Gray );
		}
	}


	private void SetAuthorizedState()
	{
		SetStatus( "✓ Connected to GitHub. Click the button below to re-authorize.", WpfBrushes.Green );
		AuthorizeButton.Content = "Re-authorize with GitHub...";
	}


	private void SetStatus( string message, WpfBrush color )
	{
		AuthStatusText.Text = message;
		AuthStatusText.Foreground = color;
	}


	private async void AuthorizeButton_Click( object sender, RoutedEventArgs e )
	{
		var clientId = ClientIdTextBox.Text.Trim();

		if ( string.IsNullOrWhiteSpace( clientId ) )
		{
			SetStatus( "Please enter your GitHub OAuth App Client ID first.", WpfBrushes.DarkOrange );
			ClientIdTextBox.Focus();
			return;
		}

		AuthorizeButton.IsEnabled = false;
		CodePanel.Visibility = Visibility.Collapsed;
		SetStatus( "Requesting device code from GitHub...", WpfBrushes.Gray );

		m_PollCts?.Cancel();
		m_PollCts = new CancellationTokenSource();

		var authService = new GitHubAuthService( clientId );

		try
		{
			var deviceResponse = await authService.RequestDeviceCodeAsync( m_PollCts.Token ).ConfigureAwait( true );

			UserCodeText.Text = deviceResponse.UserCode;
			CodePanel.Visibility = Visibility.Visible;
			CopyToClipboard( deviceResponse.UserCode );

			SetStatus( "Device code copied to clipboard. Paste it in the browser page that just opened.", WpfBrushes.DarkOrange );

			Process.Start( new ProcessStartInfo( deviceResponse.VerificationUri ) { UseShellExecute = true } );

			var token = await authService.PollForTokenAsync(
				deviceResponse.DeviceCode,
				deviceResponse.Interval,
				status => Dispatcher.Invoke( () => SetStatus( status, WpfBrushes.DarkOrange ) ),
				m_PollCts.Token ).ConfigureAwait( true );

			m_PendingAccessToken = token;
			CodePanel.Visibility = Visibility.Collapsed;
			SetStatus( "✓ Authorization successful! Click Save to apply.", WpfBrushes.Green );
			AuthorizeButton.Content = "Re-authorize with GitHub...";
		}
		catch ( OperationCanceledException )
		{
			SetStatus( "Authorization cancelled.", WpfBrushes.Gray );
		}
		catch ( Exception ex )
		{
			SetStatus( ex.Message, WpfBrushes.Red );
			CodePanel.Visibility = Visibility.Collapsed;
		}
		finally
		{
			AuthorizeButton.IsEnabled = true;
		}
	}


	private void CopyCodeButton_Click( object sender, RoutedEventArgs e )
	{
		CopyToClipboard( UserCodeText.Text );
		SetStatus( "Code copied to clipboard. Paste it in the browser tab that opened.", WpfBrushes.DarkOrange );
	}


	private static void CopyToClipboard( string text )
	{
		try
		{ System.Windows.Clipboard.SetText( text ); }
		catch { /* clipboard locked — ignore */ }
	}


	private void SaveButton_Click( object sender, RoutedEventArgs e )
	{
		m_PollCts?.Cancel();

		var settings = SettingsService.Load();

		if ( m_PendingAccessToken != null )
		{
			settings.AccessToken = m_PendingAccessToken;
		}

		settings.GitHubClientId = ClientIdTextBox.Text.Trim();
		settings.StartWithWindows = StartupCheckBox.IsChecked == true;

		if ( IntervalCombo.SelectedItem is ComboBoxItem selected &&
			selected.Tag is string tag &&
			int.TryParse( tag, out var interval ) )
		{
			settings.RefreshIntervalMinutes = interval;
		}

		SettingsService.Save( settings );
		DialogResult = true;
		Close();
	}


	private void CancelButton_Click( object sender, RoutedEventArgs e )
	{
		m_PollCts?.Cancel();
		DialogResult = false;
		Close();
	}


	private void Hyperlink_RequestNavigate( object sender, System.Windows.Navigation.RequestNavigateEventArgs e )
	{
		Process.Start( new ProcessStartInfo( e.Uri.AbsoluteUri ) { UseShellExecute = true } );
		e.Handled = true;
	}


	protected override void OnClosed( EventArgs e )
	{
		m_PollCts?.Cancel();
		m_PollCts?.Dispose();
		base.OnClosed( e );
	}
}
